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
from opentelemetry.trace.status import Status, StatusCode


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
    def __init__(self, shutdown_event):
        self.settingsdb = SettingsDB()
        self.logger = logging.getLogger(self.__class__.__name__)

        self.request_timeout_ms = self.settingsdb.get_config('request_timeout_ms')
        self.shutdown_event = shutdown_event

        self.timer = {}

    def start_timer(self, name, count_towards_total):
        # Overlapping timers should use count_towards_total=False
        if name in self.timer:
            raise Exception(f'Using repeated timer: {name}')

        self.timer[name] = {
            'start': time.perf_counter(),
            'count_towards_total': count_towards_total,
            'end': 0,
            'duration': 0,
            'completed': False,
        }

    def end_timer(self, name):
        if name not in self.timer:
            raise Exception('Using not existent timer')

        self.timer[name]['end'] = time.perf_counter()
        self.timer[name]['duration'] = self.timer[name]['end'] - self.timer[name]['start']
        self.timer[name]['completed'] = True

    def get_timer_duration(self, name):
        if self.timer[name]['completed']:
            return self.timer[name]['duration']

        raise Exception(f'Timer {name} not completed')

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

    def _halt(self, span, page_status=None, reason='', outcome='halted', exc=None, level='info'):
        if exc is not None:
            self.logger.exception(exc)
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR))

        span.set_attribute('request.outcome', outcome)
        span.set_attribute('halt.reason', reason)

        if page_status is not None:
            span.set_attribute('page.status_after', page_status.value)
            self.pager.page.update(status=page_status)

        log_fn = {
            'info': self._log_info,
            'warning': self._log_warning,
            'error': self._log_error,
        }[level]
        log_fn(f'Halting: {reason}')
        return None

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

    def logging_timers(self, total_duration):
        total_measured = 0

        values_to_log = [('Total', total_duration)]

        for name, item in self.timer.items():
            print(
                name,
                item,
                item['count_towards_total'],
                item['completed'],
                item['count_towards_total'] and item['completed'],
            )
            if item['count_towards_total'] and item['completed']:
                total_measured += item['duration']

            x = 'T' if item['count_towards_total'] else 'F'

            if item['completed']:
                values_to_log.append((name + f' ({x})', item['duration']))
            else:
                values_to_log.append((name + f' ({x})', None))

        missing = total_duration - total_measured
        values_to_log.append(('Missing', missing))

        log_parts = []
        for name, value in values_to_log:
            if value is None:
                log_parts.append(f'{name}: None')
            else:
                log_parts.append(f'{name}: {value:.1f}s')

        self.logger.success('Timers: ' + ' | '.join(log_parts))

    def request(self, page_playwright):
        ret = None
        try:
            start_timer = time.perf_counter()
            ret = self._request(page_playwright)
            duration = time.perf_counter() - start_timer

            self.logging_timers(duration)
        except Exception as e:
            self.error(f'Uncaught exception: {str(e)}')
        finally:
            return ret

    def _request(self, page_playwright):
        self.start_timer('request.begin', count_towards_total=False)
        self.start_timer('before.goto', count_towards_total=True)
        self._log_info('Mining started')

        self.name = threading.current_thread().name

        with tracer.start_as_current_span('requester.request') as span:
            span.set_attribute('page.page_id', self.pager.page.id)
            span.set_attribute('domain.domain_id', self.pager.domain.id)
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

            max_allowed_retries = self.settingsdb.get_config('max_retry_attempts')
            if self.pager.page.retry_count >= max_allowed_retries:
                self._halt(span, page_status=PageStatus.FAILED, reason='max_retry_attempts')
                metric_requests_failed_max_retry.add(1, {'service': 'miner'})
                return

            if not self.has_more_recursion_limit(self.pager):
                self._halt(
                    span, page_status=PageStatus.BLOCKED_LIMIT_RECURSION, reason='max_recursion'
                )
                metric_requests_reached_recursion_limit.add(1, {'service': 'miner'})
                return

            if is_domain_blocked(self.url):
                metric_requests_domain_blocked.add(1, {'service': 'miner'})
                self._halt(span, page_status=PageStatus.DOMAIN_BLOCKED, reason='domain_blocked')
                return

            if not self.pager.domain.try_register_request():
                self._halt(span, page_status=PageStatus.TODO, reason='domain_cooldown')
                metric_request_domain_in_cooldown.add(1, {'service': 'miner'})
                return None
            else:
                span.set_attribute('domain.is_in_cooldown', False)

            if self.shutdown_event.is_set():
                self._halt(span, page_status=PageStatus.TODO, reason='shutdown event')
                return

            self.pager.page.update(retry_count=self.pager.page.retry_count + 1)
            self.end_timer('before.goto')
            self.start_timer('goto.block', count_towards_total=True)

            try:
                try:
                    metric_requests_made.add(1, {'service': 'miner'})

                    self.start_timer('goto.only', count_towards_total=False)
                    response = page_playwright.goto(
                        self.url,
                        wait_until='domcontentloaded',
                        timeout=self.request_timeout_ms,
                    )
                    self.end_timer('goto.only')

                    metric_page_goto_duration.record(
                        self.get_timer_duration('goto.only'),
                        {'service': 'miner'},
                    )

                except PlaywrightTimeout:
                    metric_requests_failed.add(1, {'service': 'miner'})
                    self._halt(
                        span,
                        page_status=PageStatus.FAILED_TIMEOUT,
                        reason='PlaywrightTimeout',
                        level='warning',
                    )
                    return
                except (
                    PlaywrightError,
                    TargetClosedError,
                ):
                    self._halt(
                        span,
                        page_status=PageStatus.TODO,
                        reason='Playwright error',
                        level='warning',
                    )
                    metric_requests_failed.add(1, {'service': 'miner'})
                    return

                try:
                    page_playwright.wait_for_selector(
                        'body', timeout=int(self.request_timeout_ms / 2)
                    )

                    page_playwright.wait_for_load_state(
                        'networkidle', timeout=int(self.request_timeout_ms / 3)
                    )
                except PlaywrightTimeout:
                    self._halt(
                        span,
                        page_status=PageStatus.TODO,
                        reason='PlaywrightTimeout2',
                        level='warning',
                    )
                    return

                self.end_timer('goto.block')
                self.start_timer('goto.processing', count_towards_total=True)

                final_url = page_playwright.url
                status_code = response.status if response else None

                if status_code is not None and status_code >= 400:
                    metric_requests_failed_status_code.add(1, {'service': 'miner'})
                    self._halt(
                        span,
                        page_status=PageStatus.TODO,
                        reason=f'wrong http status code {status_code}',
                    )
                    return None

                text_content = page_playwright.locator('body').inner_text()
                html_content = page_playwright.content()
                title = page_playwright.title()

                anchors = page_playwright.locator('a[href]')
                hrefs = anchors.evaluate_all('elements => elements.map(e => e.href)')

                metric_pages_saved.add(1, {'service': 'miner'})

                self.start_timer('detect_lang', count_towards_total=False)
                is_desired_lang = detect_lang(text_content)
                self.end_timer('detect_lang')

                new_status = PageStatus.DONE if is_desired_lang else PageStatus.BLOCKED_LANGUAGE

                self.end_timer('goto.processing')
                self.start_timer('page.save_results', count_towards_total=True)

                self.pager.page.update(
                    url_final=final_url,
                    status_code=status_code,
                    title=title,
                    text=text_content,
                    html=html_content,
                    status=new_status,
                )
                self.end_timer('page.save_results')

                self.start_timer('domain.bulk_save', count_towards_total=True)
                domains_created = Domain.bulk_get_or_create(hrefs, self.pager.domain)
                self.end_timer('domain.bulk_save')

                self.start_timer('pages.processing', count_towards_total=True)

                span.set_attribute('page.total_hrefs', len(hrefs))

                total_urls_saved = 0
                pages_to_insert = []
                status_counter = Counter()

                for found_url in hrefs:
                    if not is_valid_url(found_url):
                        continue

                    domain_url = Domain.extract_hostname(found_url)
                    domain = domains_created.get(domain_url, None)

                    if domain is None:
                        continue

                    new_page_recursion_level = self.pager.page.recursion_level + 1

                    if self.pager.domain.id != domain.id:
                        # if the domain changes, reset the recursion level for the page
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
                            'recursion_level': new_page_recursion_level,
                            'status': status.value,
                        }
                    )
                    status_counter[status.value] += 1
                self.start_timer('pages.bulk_save', count_towards_total=False)
                total_urls_saved = Page.bulk_insert_ignore(pages_to_insert)
                self.end_timer('pages.bulk_save')

                for status, count in status_counter.items():
                    metric_pages_saved_with_status.add(
                        count,
                        {
                            'service': 'miner',
                            'status': status,
                        },
                    )

                self.end_timer('pages.processing')

                metric_saving_found_hrefs_duration.record(
                    self.get_timer_duration('pages.processing'),
                    {'service': 'miner'},
                )

                self._log_info(f'Saved {total_urls_saved} new URLs')
                span.set_status(Status(StatusCode.OK))

                self.end_timer('request.begin')
                metric_request_duration.record(
                    self.get_timer_duration('request.begin'),
                    {'service': 'miner'},
                )

                return True

            except PlaywrightTimeout:
                self._halt(
                    span, page_status=PageStatus.TODO, reason='Timeout error3', level='warning'
                )
                return None

            except (TargetClosedError, PlaywrightError) as e:
                if self.shutdown_event and self.shutdown_event.is_set():
                    self._halt(
                        span,
                        page_status=PageStatus.TODO,
                        reason='Shutdown closed playwright',
                        level='warning',
                        exc=e,
                    )
                    return None
                self._halt(
                    span,
                    page_status=PageStatus.TODO,
                    reason='TargetClosedError, PlaywrightError',
                    level='warning',
                    exc=e,
                )
                return None

            except AttributeError as e:
                self._halt(
                    span,
                    page_status=PageStatus.TODO,
                    reason='AttributeError as e',
                    level='warning',
                    exc=e,
                )
                raise e

            except KeyboardInterrupt:
                self._halt(
                    span, page_status=PageStatus.TODO, reason='KeyboardInterrupt', level='info'
                )
                return None

            except Exception as e:
                self._halt(
                    span,
                    page_status=PageStatus.FAILED,
                    reason='Generic Exception',
                    level='error',
                    exc=e,
                )
                return None

            finally:
                pass

            self._halt(
                span,
                page_status=PageStatus.FAILED,
                reason='Generic Exception',
                level='error',
            )
            return None
