import requests
from miner.filters import is_domain_blocked
from miner.enums import PageStatus
from miner.settings.settings_db import SettingsDB
import logging
from urllib.parse import urlparse
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeout
from miner.pager.pager import Pager
from miner.models.utils import is_valid_url

# TODO: usar o request_timeout_ms
# TODO: colocar um filter de idioma no text
class Requester:
    def __init__(self):
        self.settingsdb = SettingsDB()
        self.logger = logging.getLogger(self.__class__.__name__)

    def prepare(self, pager):
        self.pager = pager
        self.url = self.pager.page.url

    def _build_context(self, browser):
        user_agent = (
            "Mozilla/5.0 (X11; Linux x86_64) "
            "AppleWebKit/537.36 (KHTML, like Gecko) "
            "Chrome/122.0.0.0 Safari/537.36"
        )

        return browser.new_context(
            user_agent=user_agent,
            java_script_enabled=True,
            ignore_https_errors=True,
            viewport={"width": 1366, "height": 768},
        )


    def _block_unneeded_resources(self, page):
        def route_handler(route):
            resource_type = route.request.resource_type
            if resource_type in {"image", "media", "font"}:
                route.abort()
            else:
                route.continue_()

        page.route("**/*", route_handler)


    def _log_context(self, **extra):
        context = {
            'domain/page_ids': f'{self.pager.domain.id}/{self.pager.page.id}',
            'url': self.url,
            'domain': self.pager.domain,
            'page_retry_count': self.pager.page.retry_count,
            'domain/page_recursion_level': f'{self.pager.domain.recursion_level}/{self.pager.page.recursion_level}',
        }
        context.update(extra)
        return context

    def _log_info(self, message, **extra):
        self.logger.info('%s | context=%s', message, self._log_context(**extra))

    def _log_warning(self, message, **extra):
        self.logger.warning('%s | context=%s', message, self._log_context(**extra))

    def _log_error(self, message, **extra):
        self.logger.error('%s | context=%s', message, self._log_context(**extra))

    def has_more_recursion_limit(self, pager):
        max_recursion = self.settingsdb.get_config('max_recursion')
        if pager.domain.recursion_level < max_recursion and pager.page_recursion_level < max_recursion:
            return True

        return False

    def request(self):
        self._log_info(f'Mining - {self.url}')

        max_allowed_retries = self.settingsdb.get_config('max_retry_attempts')
        if self.pager.page.retry_count >= max_allowed_retries:
            self.pager.page.update(status=PageStatus.FAILED)
            self._log_info(f'Too many retries. Status set to {PageStatus.FAILED}')
            return


        # max_recursion = self.settingsdb.get_config('max_recursion')
        # if self.pager.domain.recursion_level >= max_recursion or self.pager.page.recursion_level >= max_recursion:
        #     self.pager.page.update(status=PageStatus.BLOCKED_LIMIT_RECURSION)
        #     self._log_info(
        #         f'Max recursion reached. Status set to {PageStatus.BLOCKED_LIMIT_RECURSION}',
        #         extra={
        #             'max_recursion': max_recursion
        #         }
        #     )
        #     return

        if not self.has_more_recursion_limit(self.pager):
            self.pager.page.update(status=PageStatus.BLOCKED_LIMIT_RECURSION)
            self._log_info(
                f'Max recursion reached. Status set to {PageStatus.BLOCKED_LIMIT_RECURSION}',
                extra={
                    'max_recursion': max_recursion
                }
            )
            return

        if is_domain_blocked(self.url):
            self.pager.page.update(status=PageStatus.DOMAIN_BLOCKED)
            self._log_info('Halting: domain BLOCKED')
            return

        context = None
        page = None

        if not self.pager.domain.try_register_request():
            self.pager.page.update(status=PageStatus.TODO)
            self._log_info('Halting: domain in COOLDOWN')
            return None

        self.pager.page.update(retry_count=self.pager.page.retry_count + 1)
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(
                    headless=True,
                    args=[
                        '--disable-blink-features=AutomationControlled',
                        '--no-sandbox',
                        '--disable-dev-shm-usage',
                    ],
                )

                context = self._build_context(browser)
                page = context.new_page()
                self._block_unneeded_resources(page)

                response = page.goto(
                    self.url,
                    wait_until='domcontentloaded',
                    timeout=30000,
                )

                page.wait_for_selector('body', timeout=15000)

                try:
                    page.wait_for_load_state('networkidle', timeout=5000)
                except PlaywrightTimeout:
                    pass

                final_url = page.url
                status_code = response.status if response else None

                if status_code is not None and status_code >= 400:
                    self.pager.page.update(status=PageStatus.TODO)
                    return None

                text_content = page.locator('body').inner_text()
                html_content = page.content()
                title = page.title()

                anchors = page.locator('a[href]')
                hrefs = anchors.evaluate_all('elements => elements.map(e => e.href)')

                self.pager.page.update(
                    url_final=final_url,
                    status_code=status_code,
                    title=title,
                    text=text_content,
                    html=html_content,
                    status=PageStatus.DONE,
                )

                total_urls_saved = 0
                for found_url in hrefs:
                    if is_valid_url(found_url):
                        created_page = Pager(url=found_url, parent=self.pager)
                        total_urls_saved += 1

                        if not self.has_more_recursion_limit(created_page):
                            created_page.save(PageStatus.BLOCKED_LIMIT_RECURSION)
                        elif is_domain_blocked(created_page.url):
                            created_page.save(PageStatus.DOMAIN_BLOCKED)
                        else:
                            created_page.save(PageStatus.TODO)

                self._log_info(f'Saved {total_urls_saved} new URLs')

                context.close()
                browser.close()
                return True


        except PlaywrightTimeout:
            self._log_warning('Timeout error')
            self.pager.page.update(status=PageStatus.FAILED)
            return None

        except Exception as e:
            self.logger.exception(f'{e}')
            self.pager.page.update(status=PageStatus.FAILED)
            return None


        self._log_error('This log should not be reached.')
        return None
