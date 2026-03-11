from miner.filters import is_domain_blocked, detect_lang
from miner.enums import PageStatus
from miner.settings.settings_db import SettingsDB
import logging
from playwright.sync_api import TimeoutError as PlaywrightTimeout, Error as PlaywrightError
from miner.models.utils import is_valid_url
from opentelemetry import trace
from playwright._impl._errors import TargetClosedError
import time
import threading
from collections import Counter
from miner.models.utils import md5_bin16
from miner.models import Page, Domain


from miner.metrics import (
    metric_requests_started,
    metric_requests_made,
    metric_requests_failed_max_retry,
    metric_requests_reached_recursion_limit,
    metric_requests_domain_blocked,
    metric_requests_failed,
    metric_requests_failed_status_code,
    metric_pages_saved,
    metric_pages_saved_with_status,
    metric_request_duration,
    metric_page_goto_duration,
    metric_request_domain_in_cooldown,
    metric_saving_found_hrefs_duration,
)


tracer = trace.get_tracer(__name__)


class Requester:
    def __init__(self, shutdown_event=None):
        self.settingsdb = SettingsDB()
        self.logger = logging.getLogger(self.__class__.__name__)

        self.request_timeout_ms = self.settingsdb.get_config('request_timeout_ms')
        self.shutdown_event = shutdown_event

    def prepare(self, pager):
        self.pager = pager
        self.url = self.pager.url

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

    def _log_success(self, message, **extra):
        self.logger.success('%s | context=%s', message, self._log_context(**extra))

    def has_more_recursion_limit(self, pager):
        max_recursion = self.settingsdb.get_config('max_recursion')
        max_recursion_page = self.settingsdb.get_config('max_recursion_page')

        if pager.domain.recursion_level >= max_recursion:
            return False

        if pager.page_recursion_level >= max_recursion_page:
            return False

        return True

    def has_more_recursion_limit_specific(self, domain_recursion_level, page_recursion_level):
        max_recursion = self.settingsdb.get_config('max_recursion')
        max_recursion_page = self.settingsdb.get_config('max_recursion_page')

        if domain_recursion_level >= max_recursion:
            return False

        if page_recursion_level >= max_recursion_page:
            return False

        return True

    def request(self, page_playwright):
        start_timer_request = time.perf_counter()

        start_pre_goto = time.perf_counter()
        self.name = threading.current_thread().name

        with tracer.start_as_current_span('requester.request') as span:
            span.set_attribute('page.url', self.url)
            span.set_attribute('thread.name', self.name)
            span.set_attribute('page.status_before', str(self.pager.page.status))
            span.set_attribute(
                'page.recursion_level', getattr(self.pager, 'page_recursion_level', 0)
            )
            span.set_attribute(
                'domain.recursion_level', getattr(self.pager.domain, 'recursion_level', 0)
            )
            span.set_attribute('page.retry_count', self.pager.page.retry_count)

            metric_requests_started.add(1, {'service': 'miner'})
            self._log_info('Mining')

            max_allowed_retries = self.settingsdb.get_config('max_retry_attempts')
            if self.pager.page.retry_count >= max_allowed_retries:
                span.set_attribute('status', PageStatus.FAILED.value)
                metric_requests_failed_max_retry.add(1, {'service': 'miner'})
                self.pager.page.update(status=PageStatus.FAILED)
                self._log_info(f'Halting: Too many retries. Status set to {PageStatus.FAILED}')
                return

            max_recursion = self.settingsdb.get_config('max_recursion')
            if not self.has_more_recursion_limit(self.pager):
                span.set_attribute('status', PageStatus.BLOCKED_LIMIT_RECURSION.value)
                metric_requests_reached_recursion_limit.add(1, {'service': 'miner'})
                self.pager.page.update(status=PageStatus.BLOCKED_LIMIT_RECURSION)
                self._log_info(
                    f'Halting: Max recursion reached. Status set to {PageStatus.BLOCKED_LIMIT_RECURSION.value}',
                    extra={'max_recursion': max_recursion},
                )
                return

            with tracer.start_as_current_span('requester.blocked_domain'):
                if is_domain_blocked(self.url):
                    span.set_attribute('status', PageStatus.DOMAIN_BLOCKED.value)
                    metric_requests_domain_blocked.add(1, {'service': 'miner'})
                    self.pager.page.update(status=PageStatus.DOMAIN_BLOCKED)
                    self._log_info('Halting: domain BLOCKED')
                    return

            if not self.pager.domain.try_register_request():
                span.set_attribute('domain.is_in_cooldown', True)
                metric_request_domain_in_cooldown.add(1, {'service': 'miner'})
                self.pager.page.update(status=PageStatus.TODO)
                self._log_info('Halting: domain in COOLDOWN')
                return None
            else:
                span.set_attribute('domain.is_in_cooldown', False)

            self.pager.page.update(retry_count=self.pager.page.retry_count + 1)

            if self.shutdown_event.is_set():
                self.pager.page.update(status=PageStatus.TODO)
                self._log_info('Halting: Shutdown event detected')
                return

            duration_pre_goto = time.perf_counter() - start_pre_goto

            try:
                with tracer.start_as_current_span('requester.page_goto') as span_goto:
                    try:
                        metric_requests_made.add(1, {'service': 'miner'})

                        start_timer_page_goto = time.perf_counter()
                        response = page_playwright.goto(
                            self.url,
                            wait_until='domcontentloaded',
                            timeout=self.request_timeout_ms,
                        )

                        duration_goto_load = time.perf_counter() - start_timer_page_goto
                        metric_page_goto_duration.record(
                            duration_goto_load,
                            {'service': 'miner'},
                        )

                    except PlaywrightTimeout as e:
                        span_goto.record_exception(e)
                        metric_requests_failed.add(1, {'service': 'miner'})
                        self.pager.page.update(status=PageStatus.FAILED_TIMEOUT)
                        self._log_warning(f'Halting: page.goto failed: {PageStatus.FAILED_TIMEOUT}')
                        return
                    except (
                        PlaywrightError,
                        TargetClosedError,
                    ) as e:
                        span_goto.record_exception(e)
                        metric_requests_failed.add(1, {'service': 'miner'})
                        self.pager.page.update(status=PageStatus.TODO)
                        self._log_warning(f'Halting: page.goto failed: {type(e).__name__}')
                        return

                    page_playwright.wait_for_selector(
                        'body', timeout=int(self.request_timeout_ms / 2)
                    )

                    try:
                        page_playwright.wait_for_load_state(
                            'networkidle', timeout=int(self.request_timeout_ms / 3)
                        )
                    except PlaywrightTimeout:
                        pass

                    final_url = page_playwright.url
                    status_code = response.status if response else None

                    span_goto.set_attribute('http.response.status_code', status_code)
                    span_goto.set_attribute('page.final_url', final_url)

                    if status_code is not None and status_code >= 400:
                        metric_requests_failed_status_code.add(1, {'service': 'miner'})
                        self.pager.page.update(status=PageStatus.TODO)
                        self._log_info('Halting: wrong http status code')
                        return None

                    text_content = page_playwright.locator('body').inner_text()
                    html_content = page_playwright.content()
                    title = page_playwright.title()

                    anchors = page_playwright.locator('a[href]')
                    hrefs = anchors.evaluate_all('elements => elements.map(e => e.href)')

                metric_pages_saved.add(1, {'service': 'miner'})

                start_detect_lang = time.perf_counter()
                is_desired_lang = detect_lang(text_content)
                duration_detect_lang = time.perf_counter() - start_detect_lang

                start_update_mined_page_update = time.perf_counter()
                self.pager.page.update(
                    url_final=final_url,
                    status_code=status_code,
                    title=title,
                    text=text_content,
                    html=html_content,
                    status=PageStatus.DONE if is_desired_lang else PageStatus.BLOCKED_LANGUAGE,
                )
                duration_update_mined_page_update = (
                    time.perf_counter() - start_update_mined_page_update
                )

                start_create_domains = time.perf_counter()
                domains_created = Domain.bulk_get_or_create(hrefs, self.pager.domain)
                duration_create_domains = time.perf_counter() - start_create_domains

                span.set_attribute('page.total_hrefs', len(hrefs))

                with tracer.start_as_current_span('requester.saving_hrefs') as span_savinghrefs:
                    total_urls_saved = 0
                    start_timer_saving_hrefs = time.perf_counter()

                    pages_to_insert = []
                    status_counter = Counter()

                    for found_url in hrefs:
                        if not is_valid_url(found_url):
                            continue

                        domain_url = Domain.extract_hostname(found_url)
                        domain = domains_created.get(domain_url, None)

                        new_page_recursion_level = self.pager.page.recursion_level + 1

                        if self.pager.domain.id != domain.id:
                            new_page_recursion_level = 0

                        if not self.has_more_recursion_limit_specific(
                            domain.recursion_level, new_page_recursion_level
                        ):
                            status = PageStatus.BLOCKED_LIMIT_RECURSION
                        elif is_domain_blocked(found_url):
                            status = PageStatus.DOMAIN_BLOCKED
                        elif not is_desired_lang:
                            status = PageStatus.BLOCKED_LANGUAGE
                        else:
                            status = PageStatus.TODO

                        pages_to_insert.append(
                            {
                                'domain_id': domain.id,
                                'parent_page_id': self.pager.page.id,
                                'same_as': None,
                                'url': found_url,
                                'url_md5': md5_bin16(found_url),
                                'recursion_level': self.pager.page.recursion_level + 1,
                                'status': status.value,
                            }
                        )
                        status_counter[status.value] += 1
                    total_urls_saved = Page.bulk_insert_ignore(pages_to_insert)

                    for status, count in status_counter.items():
                        metric_pages_saved_with_status.add(
                            count,
                            {
                                'service': 'miner',
                                'status': status,
                            },
                        )
                    span_savinghrefs.set_attribute('total_saved', total_urls_saved)

                    metric_saving_found_hrefs_duration.record(
                        (time.perf_counter() - start_timer_saving_hrefs),
                        {'service': 'miner'},
                    )

                    duration_saving_hrefs = time.perf_counter() - start_timer_saving_hrefs

                self._log_info(f'Saved {total_urls_saved} new URLs')

                duration_entire_method = time.perf_counter() - start_timer_request
                metric_request_duration.record(
                    duration_entire_method,
                    {'service': 'miner'},
                )

                missing = (
                    duration_entire_method
                    - duration_saving_hrefs
                    - duration_goto_load
                    - duration_detect_lang
                    - duration_update_mined_page_update
                    - duration_pre_goto
                    - duration_create_domains
                )

                pct_saving = (duration_saving_hrefs / duration_entire_method) * 100
                pct_goto = (duration_goto_load / duration_entire_method) * 100
                pct_lang = (duration_detect_lang / duration_entire_method) * 100
                pct_update = (duration_update_mined_page_update / duration_entire_method) * 100
                pct_duration_pre_goto = (duration_pre_goto / duration_entire_method) * 100
                pct_create_domains = (duration_create_domains / duration_entire_method) * 100
                pct_missing = (missing / duration_entire_method) * 100

                self.logger.success(
                    'Complete mined URL! | '
                    f'Total: {duration_entire_method:.3f}s | '
                    f'HREFs: {pct_saving:.1f}% | '
                    f'Goto: {pct_goto:.1f}% | '
                    f'Lang: {pct_lang:.1f}% | '
                    f'Update: {pct_update:.1f}% | '
                    f'pregoto: {pct_duration_pre_goto:.1f}% | '
                    f'domains: {pct_create_domains:.1f}% | '
                    f'Missing: {pct_missing:.1f}%'
                )

                return True

            except PlaywrightTimeout:
                self._log_warning('Halting: Timeout error')
                self.pager.page.update(status=PageStatus.TODO)
                return None

            except (TargetClosedError, PlaywrightError):
                if self.shutdown_event and self.shutdown_event.is_set():
                    self._log_warning('Halting: Playwright encerrado durante shutdown')
                    self.pager.page.update(status=PageStatus.TODO)
                    return None

                self._log_warning('Halting: Playwright falhou fora do shutdown')
                self.pager.page.update(status=PageStatus.TODO)
                return None

            except AttributeError as e:
                if '_playwright' in str(e):
                    if self.shutdown_event and self.shutdown_event.is_set():
                        self._log_warning(
                            'Halting: Playwright initialization interrupted during shutdown'
                        )
                        self.pager.page.update(status=PageStatus.TODO)
                        return None
                    self._log_warning('Halting: Playwright initialization failed')
                    self.pager.page.update(status=PageStatus.TODO)
                    return None
                raise

            except KeyboardInterrupt:
                self._log_info('Halting: Interrompido por sinal')
                self.pager.page.update(status=PageStatus.TODO)
                return None

            except Exception as e:
                self._log_error('Halting: Generic captured exception', extra={'exception': e})
                self.logger.exception('Halting: Generic captured exception')
                self.pager.page.update(status=PageStatus.FAILED)
                return None

            finally:
                pass

            self._log_error('This log should not be reached.')
            return None
