"""Request execution and page extraction logic."""

import logging
import threading
import time
from collections import Counter
from contextlib import contextmanager
from html.parser import HTMLParser
from urllib.parse import urljoin

from opentelemetry import trace
from opentelemetry.trace.status import Status, StatusCode
from playwright._impl._errors import TargetClosedError
from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import TimeoutError as PlaywrightTimeout

from miner.enums import PageStatus
from miner.filters import detect_lang, is_domain_blocked
from miner.metrics import (
    metric_page_goto_duration,
    metric_pages_saved,
    metric_pages_saved_with_status,
    metric_request_domain_in_cooldown,
    metric_request_duration,
    metric_requests_domain_blocked,
    metric_requests_failed,
    metric_requests_failed_max_retry,
    metric_requests_failed_status_code,
    metric_requests_made,
    metric_requests_reached_recursion_limit,
    metric_requests_started,
    metric_saving_found_hrefs_duration,
)
from miner.models import Domain, Page
from miner.models.utils import is_valid_url
from miner.settings.settings import settings
from miner.settings.settings_db import SettingsDB

tracer = trace.get_tracer(__name__)

_SUPPORTED_PREFLIGHT_CONTENT_TYPES = (
    'text/',
    'application/json',
    'application/ld+json',
    'application/xml',
    'application/xhtml+xml',
    'application/rss+xml',
    'application/atom+xml',
)


class _HTMLTextExtractor(HTMLParser):
    """Extract visible-ish text from HTML without adding a runtime dependency."""

    def __init__(self):
        super().__init__()
        self._ignored_tag_depth = 0
        self._parts = []

    def handle_starttag(self, tag, attrs):  # pylint: disable=unused-argument
        if tag in {'script', 'style', 'noscript'}:
            self._ignored_tag_depth += 1

    def handle_endtag(self, tag):
        if tag in {'script', 'style', 'noscript'} and self._ignored_tag_depth:
            self._ignored_tag_depth -= 1

    def handle_data(self, data):
        if self._ignored_tag_depth:
            return
        text = data.strip()
        if text:
            self._parts.append(text)

    def get_text(self):
        """Return extracted text."""
        return ' '.join(self._parts)


class _HTMLHrefExtractor(HTMLParser):
    """Extract links from fallback HTML."""

    def __init__(self, base_url):
        super().__init__()
        self._base_url = base_url
        self.hrefs = []

    def handle_starttag(self, tag, attrs):
        if tag != 'a':
            return

        attrs_dict = dict(attrs)
        href = attrs_dict.get('href')
        if href:
            self.hrefs.append(urljoin(self._base_url, href))


class Requester:  # pylint: disable=too-many-instance-attributes
    """Execute a single page request and persist extracted results."""

    def __init__(self, shutdown_event):
        """Initialize a requester bound to a shutdown event."""
        self.settingsdb = SettingsDB()
        self.logger = logging.getLogger(self.__class__.__name__)

        self.request_timeout_ms = self.settingsdb.get_config('request_timeout_ms')
        self.shutdown_event = shutdown_event

        self.timer = {}
        self.pager = None
        self.url = None
        self.name = None

    def start_timer(self, name, count_towards_total):
        """Start a named timer."""
        # Overlapping timers should use count_towards_total=False
        if name in self.timer:
            raise RuntimeError(f'Using repeated timer: {name}')

        self.timer[name] = {
            'start': time.perf_counter(),
            'count_towards_total': count_towards_total,
            'end': 0,
            'duration': 0,
            'completed': False,
        }

    def end_timer(self, name):
        """End a named timer."""
        if name not in self.timer:
            raise RuntimeError('Using not existent timer')

        self.timer[name]['end'] = time.perf_counter()
        self.timer[name]['duration'] = self.timer[name]['end'] - self.timer[name]['start']
        self.timer[name]['completed'] = True

    def get_timer_duration(self, name):
        """Return the duration of a completed timer."""
        if self.timer[name]['completed']:
            return self.timer[name]['duration']

        raise RuntimeError(f'Timer {name} not completed')

    def prepare(self, pager):
        """Attach the pager that will be mined."""
        self.pager = pager
        self.url = self.pager.url

    def filter_valids_hrefs(self, hrefs):
        """Filter extracted hrefs down to valid URLs."""
        return [href for href in hrefs if is_valid_url(href)]

    def _ensure_body_available(self, page_playwright, span):
        """Wait for body, accepting CDP fallbacks when selector lookup is flaky."""
        try:
            body_timeout_ms = int(self.request_timeout_ms / 2)
            with self._monitored_step('wait_for_selector_body', timeout_ms=body_timeout_ms):
                page_playwright.wait_for_selector('body', timeout=body_timeout_ms)
            return True
        except PlaywrightTimeout:
            span.set_attribute('playwright.body.wait_for_selector.timeout', True)

        try:
            with self._monitored_step('evaluate_body_presence'):
                has_body = page_playwright.evaluate('() => document.body !== null')
            if has_body:
                span.set_attribute('playwright.body.fallback', 'document.body')
                return True
        except PlaywrightError as e:
            span.set_attribute('playwright.body.fallback.evaluate_error', str(e)[:500])

        try:
            with self._monitored_step('evaluate_document_presence'):
                has_document = page_playwright.evaluate(
                    '() => document.documentElement !== null'
                )
            if has_document:
                span.set_attribute('playwright.body.fallback', 'document.documentElement')
                return True
        except PlaywrightError as e:
            span.set_attribute('playwright.document.fallback.evaluate_error', str(e)[:500])

        if settings.SAVE_HTML:
            try:
                with self._monitored_step('read_content_for_body_fallback'):
                    html_content = page_playwright.content()
                if '<body' in html_content.lower():
                    span.set_attribute('playwright.body.fallback', 'page.content')
                    return True
            except PlaywrightError as e:
                span.set_attribute('playwright.body.fallback.content_error', str(e)[:500])
        else:
            span.set_attribute('playwright.body.page_content_fallback_skipped', True)

        return False

    def _read_body_text(self, page_playwright, span):
        """Read body text with a JavaScript fallback for CDP compatibility."""
        try:
            with self._monitored_step('read_body_inner_text', timeout_ms=2000):
                return page_playwright.locator('body').inner_text(timeout=2000)
        except (PlaywrightTimeout, PlaywrightError) as e:
            span.set_attribute('playwright.body.inner_text_error', str(e)[:500])

        try:
            with self._monitored_step('evaluate_body_text'):
                text = page_playwright.evaluate(
                    '() => { const node = document.body || document.documentElement; '
                    "return node ? (node.innerText || node.textContent || '') : ''; }"
                )
            span.set_attribute('playwright.body.text_source', 'evaluate')
            return text if isinstance(text, str) else str(text or '')
        except PlaywrightError as e:
            span.set_attribute('playwright.body.evaluate_text_error', str(e)[:500])
            raise

    def _read_html_content(self, page_playwright, span):
        """Read full HTML with a JavaScript fallback for CDP compatibility."""
        if not settings.SAVE_HTML:
            span.set_attribute('playwright.content.skipped', 'SAVE_HTML=false')
            self._log_info('Skipping HTML content read', save_html=settings.SAVE_HTML)
            return None

        try:
            with self._monitored_step('read_page_content'):
                html_content = page_playwright.content()
            if isinstance(html_content, str):
                return html_content
            span.set_attribute('playwright.content.unexpected_type', type(html_content).__name__)
        except PlaywrightError as e:
            span.set_attribute('playwright.content.error', str(e)[:500])

        try:
            with self._monitored_step('evaluate_page_content'):
                html_content = page_playwright.evaluate(
                    '() => document.documentElement '
                    '? document.documentElement.outerHTML '
                    ": ''"
                )
            span.set_attribute('playwright.content.source', 'evaluate')
            return html_content if isinstance(html_content, str) else str(html_content or '')
        except PlaywrightError as e:
            span.set_attribute('playwright.content.fallback_error', str(e)[:500])
            return ''

    @staticmethod
    def _html_to_text(html_content):
        """Convert fallback HTML into text for language detection and persistence."""
        parser = _HTMLTextExtractor()
        parser.feed(html_content)
        return parser.get_text()

    def _extract_hrefs_from_html(self, html_content, base_url):
        """Extract valid hrefs from fallback HTML."""
        parser = _HTMLHrefExtractor(base_url)
        parser.feed(html_content)
        return self.filter_valids_hrefs(parser.hrefs)

    def _read_http_fallback_content(self, page_playwright, final_url, span):
        """Fetch final URL directly when CDP cannot expose a document body."""
        if not final_url.startswith(('http://', 'https://')):
            return None, None, None

        try:
            with self._monitored_step('http_fallback_content_get'):
                response = page_playwright.context.request.get(final_url)
            status_code = response.status
            span.set_attribute('playwright.body.fallback', 'http_request')
            span.set_attribute('playwright.body.fallback_status_code', status_code)

            if status_code >= 400:
                return status_code, None, None

            with self._monitored_step('http_fallback_content_text'):
                html_content = response.text()
            text_content = self._html_to_text(html_content)
            return status_code, html_content, text_content
        except PlaywrightError as e:
            span.set_attribute('playwright.body.http_fallback_error', str(e)[:500])
            return None, None, None

    @staticmethod
    def _is_supported_preflight_content_type(content_type: str | None) -> bool:
        normalized = (content_type or '').split(';', maxsplit=1)[0].strip().lower()
        return not normalized or normalized.startswith(_SUPPORTED_PREFLIGHT_CONTENT_TYPES)

    def _run_preflight_http(
        self, page_playwright, span
    ) -> tuple[bool, int | None, str | None]:
        if not settings.PREFLIGHT_ENABLED:
            span.set_attribute('preflight.enabled', False)
            return True, None, None

        span.set_attribute('preflight.enabled', True)
        started_at = time.perf_counter()
        timeout_ms = settings.PREFLIGHT_TIMEOUT_MS
        try:
            with self._monitored_step('preflight_request_get', timeout_ms=timeout_ms):
                response = page_playwright.context.request.get(self.url, timeout=timeout_ms)

            duration_s = time.perf_counter() - started_at
            status_code = response.status
            content_type = response.headers.get('content-type', '')
            final_url = getattr(response, 'url', self.url)

            if status_code >= 400:
                outcome = 'blocked_status_code'
                reason = f'wrong http status code {status_code}'
            elif not self._is_supported_preflight_content_type(content_type):
                outcome = 'blocked_content_type'
                reason = f'unsupported content type {content_type}'
            else:
                outcome = 'continue'
                reason = None

            span.set_attribute('preflight.status_code', status_code)
            span.set_attribute('preflight.content_type', content_type)
            span.set_attribute('preflight.duration_s', duration_s)
            span.set_attribute('preflight.final_url', final_url)
            span.set_attribute('preflight.outcome', outcome)
            log_message = 'Preflight allowed browser navigation'
            if reason is not None:
                log_message = 'Preflight blocked browser navigation'
            self._log_info(
                log_message,
                preflight_outcome=outcome,
                preflight_status_code=status_code,
                preflight_content_type=content_type,
                preflight_duration_s=round(duration_s, 3),
            )
            return reason is None, status_code, reason
        except (PlaywrightTimeout, PlaywrightError) as exc:
            duration_s = time.perf_counter() - started_at
            span.set_attribute('preflight.outcome', 'unavailable')
            span.set_attribute('preflight.duration_s', duration_s)
            span.set_attribute('preflight.error', str(exc)[:500])
            self._log_info(
                'Preflight unavailable; continuing to browser navigation',
                preflight_outcome='unavailable',
                preflight_error=str(exc)[:500],
                preflight_duration_s=round(duration_s, 3),
            )
            return True, None, None

    def _read_page_title(self, page_playwright, span):
        """Read page title without failing the request on Obscura CDP quirks."""
        try:
            with self._monitored_step('read_page_title'):
                title = page_playwright.title()
            if isinstance(title, str):
                return title
            span.set_attribute('playwright.title.unexpected_type', type(title).__name__)
        except PlaywrightError as e:
            span.set_attribute('playwright.title.error', str(e)[:500])

        try:
            with self._monitored_step('evaluate_page_title'):
                title = page_playwright.evaluate("() => document.title || ''")
            span.set_attribute('playwright.title.source', 'evaluate')
            return title if isinstance(title, str) else str(title or '')
        except PlaywrightError as e:
            span.set_attribute('playwright.title.fallback_error', str(e)[:500])
            return ''

    def _log_context(self, **extra):
        """Build structured context for request logs."""
        context = {
            'domain/page_ids': f'{self.pager.domain.id}/{self.pager.page.id}',
            'url': self.url,
            'domain': self.pager.domain,
            'page_retry_count': self.pager.page.retry_count,
            'domain/page_recursion_level': (
                f'{self.pager.domain.recursion_level}/{self.pager.page.recursion_level}'
            ),
        }
        context.update(extra)
        return context

    def _log_info(self, message, **extra):
        """Log an informational message with request context."""
        self.logger.info('%s | context=%s', message, self._log_context(**extra))

    def _log_warning(self, message, **extra):
        """Log a warning message with request context."""
        self.logger.warning('%s | context=%s', message, self._log_context(**extra))

    def _log_error(self, message, **extra):
        """Log an error message with request context."""
        self.logger.error('%s | context=%s', message, self._log_context(**extra))

    def _log_success(self, message, **extra):
        """Log a success message with request context."""
        self.logger.success('%s | context=%s', message, self._log_context(**extra))

    @contextmanager
    def _monitored_step(self, step_name, timeout_ms=None, **extra):
        """Log slow request steps while the worker is blocked inside a sync call."""
        started_at = time.perf_counter()
        stop_event = threading.Event()
        context = {
            'step': step_name,
            'timeout_ms': timeout_ms,
        }
        context.update(extra)

        self._log_info('Request step started', **context)

        def monitor():
            if stop_event.wait(5):
                return

            while not stop_event.is_set():
                elapsed = time.perf_counter() - started_at
                self._log_warning(
                    'Request step still running',
                    **context,
                    elapsed_s=round(elapsed, 1),
                )
                if stop_event.wait(10):
                    return

        monitor_thread = threading.Thread(
            target=monitor,
            name=f'{threading.current_thread().name}-{step_name}-watchdog',
            daemon=True,
        )
        monitor_thread.start()

        try:
            yield
        finally:
            stop_event.set()
            duration = time.perf_counter() - started_at
            log_context = dict(context)
            log_context['duration_s'] = round(duration, 3)
            self._log_info('Request step finished', **log_context)

    def _max_retry_attempts(self):
        """Return the configured retry limit."""
        return self.settingsdb.get_config('max_retry_attempts')

    def _is_retry_limit_reached(self) -> bool:
        """Return whether the current page reached the retry limit."""
        return self.pager.page.retry_count >= self._max_retry_attempts()

    def _log_outcome(
        self,
        message,
        *,
        page_status=None,
        reason='',
        outcome='halted',
        level=None,
        exc=None,
        **extra,
    ):  # pylint: disable=too-many-arguments,too-many-locals,too-many-branches
        """Log a request outcome with severity derived from the final page state."""
        effective_status = page_status
        if effective_status is None and self.pager.page.status is not None:
            try:
                effective_status = PageStatus(self.pager.page.status)
            except ValueError:
                effective_status = self.pager.page.status

        status_value = (
            effective_status.value if isinstance(effective_status, PageStatus) else effective_status
        )
        max_retry_attempts = self._max_retry_attempts()
        context = {
            'outcome': outcome,
            'reason': reason,
            'status_after': status_value,
            'retry_count': self.pager.page.retry_count,
            'max_retry_attempts': max_retry_attempts,
            'page_id': self.pager.page.id,
            'domain_id': self.pager.domain.id,
        }
        context.update(extra)

        if level is None:
            if effective_status == PageStatus.DONE:
                level = 'success'
            elif effective_status in {PageStatus.TODO, PageStatus.FAILED_TIMEOUT}:
                level = 'warning'
            elif effective_status == PageStatus.FAILED:
                level = 'error' if self.pager.page.retry_count >= max_retry_attempts else 'warning'
            elif effective_status in {
                PageStatus.BLOCKED_LANGUAGE,
                PageStatus.DOMAIN_BLOCKED,
                PageStatus.BLOCKED_LIMIT_RECURSION,
            }:
                level = 'info'
            else:
                level = 'info'

        log_fn = {
            'debug': self.logger.debug,
            'info': self._log_info,
            'warning': self._log_warning,
            'error': self._log_error,
            'success': self._log_success,
        }[level]

        if exc is not None:
            exc_info = (type(exc), exc, exc.__traceback__)
            log_message = '%s | context=%s'
            log_context = self._log_context(**context)
            if level == 'debug':
                self.logger.debug(log_message, message, log_context, exc_info=exc_info)
            elif level == 'info':
                self.logger.info(log_message, message, log_context, exc_info=exc_info)
            elif level == 'warning':
                self.logger.warning(log_message, message, log_context, exc_info=exc_info)
            elif level == 'error':
                self.logger.error(log_message, message, log_context, exc_info=exc_info)
            else:
                self.logger.success(log_message, message, log_context, exc_info=exc_info)
        elif level == 'debug':
            self.logger.debug('%s | context=%s', message, self._log_context(**context))
        else:
            log_fn(message, **context)

    def _halt(
        self, span, page_status=None, reason='', outcome='halted', exc=None, level=None
    ):  # pylint: disable=too-many-arguments,too-many-positional-arguments
        """Record a halted request and optionally update page status."""
        if exc is not None:
            span.record_exception(exc)
            span.set_status(Status(StatusCode.ERROR))

        span.set_attribute('request.outcome', outcome)
        span.set_attribute('halt.reason', reason)

        if page_status is not None:
            span.set_attribute('page.status_after', page_status.value)
            self.pager.page.update(status=page_status)

        self._log_outcome(
            f'Halting: {reason}',
            page_status=page_status,
            reason=reason,
            outcome=outcome,
            level=level,
            exc=exc,
        )

    def has_more_recursion_limit(self, pager):
        """Return whether the page/domain pair may recurse further."""
        max_recursion = self.settingsdb.get_config('max_recursion')
        max_recursion_page = self.settingsdb.get_config('max_recursion_page')

        if pager.domain.recursion_level >= max_recursion:
            return False

        if pager.page_recursion_level >= max_recursion_page:
            return False

        return True

    def has_more_recursion_limit_specific(self, domain_recursion_level, page_recursion_level):
        """Return whether explicit recursion levels are within limits."""
        max_recursion = self.settingsdb.get_config('max_recursion')
        max_recursion_page = self.settingsdb.get_config('max_recursion_page')

        if domain_recursion_level >= max_recursion:
            return False

        if page_recursion_level >= max_recursion_page:
            return False

        return True

    def logging_timers(self, total_duration):
        """Log all request timers and unmeasured time."""
        total_measured = 0

        values_to_log = [('Total', total_duration)]

        for name, item in self.timer.items():
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

        self.logger.debug('Timers: %s', ' | '.join(log_parts))

    def request(self, page_playwright):
        """Run the request and always emit timing information."""
        ret = None
        start_timer = time.perf_counter()
        try:
            ret = self._request(page_playwright)
        except Exception as e:  # pylint: disable=broad-exception-caught
            self._log_error(f'Uncaught exception: {str(e)}')

        duration = time.perf_counter() - start_timer
        self.logging_timers(duration)
        return ret

    def _request(  # pylint: disable=too-many-locals,too-many-return-statements,too-many-branches,too-many-statements,inconsistent-return-statements,broad-exception-caught
        self, page_playwright
    ):
        """Navigate, extract content, persist the page, and enqueue discovered links."""
        self.start_timer('request.begin', count_towards_total=False)
        self.start_timer('before.goto', count_towards_total=True)
        self._log_info(
            'Mining started',
            request_timeout_ms=self.request_timeout_ms,
            preflight_enabled=settings.PREFLIGHT_ENABLED,
            preflight_timeout_ms=settings.PREFLIGHT_TIMEOUT_MS,
        )

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

            max_allowed_retries = self._max_retry_attempts()
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

            span.set_attribute('domain.is_in_cooldown', False)

            if self.shutdown_event.is_set():
                self._halt(span, page_status=PageStatus.TODO, reason='shutdown event')
                return

            self.pager.page.update(retry_count=self.pager.page.retry_count + 1)
            self.end_timer('before.goto')
            self.start_timer('goto.block', count_towards_total=True)

            try:
                (
                    should_continue_after_preflight,
                    preflight_status_code,
                    preflight_halt_reason,
                ) = self._run_preflight_http(page_playwright, span)
                if not should_continue_after_preflight:
                    self.end_timer('goto.block')
                    if (
                        preflight_status_code is not None
                        and preflight_status_code >= 400
                    ):
                        metric_requests_failed_status_code.add(1, {'service': 'miner'})
                    self._halt(
                        span,
                        page_status=PageStatus.TODO,
                        reason=preflight_halt_reason or 'preflight blocked',
                    )
                    return None

                try:
                    metric_requests_made.add(1, {'service': 'miner'})

                    response = None
                    self.start_timer('goto.only', count_towards_total=False)
                    try:
                        with self._monitored_step(
                            'page_goto_domcontentloaded',
                            timeout_ms=self.request_timeout_ms,
                            wait_until='domcontentloaded',
                        ):
                            response = page_playwright.goto(
                                self.url,
                                wait_until='domcontentloaded',
                                timeout=self.request_timeout_ms,
                            )
                    finally:
                        self.end_timer('goto.only')

                    metric_page_goto_duration.record(
                        self.get_timer_duration('goto.only'),
                        {'service': 'miner'},
                    )

                    final_url = page_playwright.url
                    status_code = response.status if response is not None else None

                    if status_code is None and final_url.startswith(('http://', 'https://')):
                        try:
                            with self._monitored_step(
                                'status_fallback_request_get',
                                final_url=final_url,
                            ):
                                fallback_response = page_playwright.context.request.get(final_url)
                            status_code = fallback_response.status
                            span.set_attribute('playwright.status_source', 'fallback_request')
                        except PlaywrightError:
                            span.set_attribute('playwright.status_source', 'unavailable')
                    else:
                        span.set_attribute('playwright.status_source', 'goto_response')

                    span.set_attribute('page.final_url', final_url)

                    if status_code is not None:
                        span.set_attribute('http.status_code', status_code)

                except PlaywrightTimeout:
                    metric_requests_failed.add(1, {'service': 'miner'})
                    self._halt(
                        span,
                        page_status=PageStatus.FAILED_TIMEOUT,
                        reason='PlaywrightTimeout',
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
                    )
                    metric_requests_failed.add(1, {'service': 'miner'})
                    return

                fallback_html_content = None
                fallback_text_content = None
                body_available = self._ensure_body_available(page_playwright, span)
                if not body_available:
                    (
                        fallback_status_code,
                        fallback_html_content,
                        fallback_text_content,
                    ) = self._read_http_fallback_content(page_playwright, final_url, span)
                    if fallback_status_code is not None:
                        status_code = fallback_status_code
                        span.set_attribute('http.status_code', status_code)

                if fallback_html_content is None and not body_available:
                    self._halt(
                        span,
                        page_status=PageStatus.TODO,
                        reason='Timeout waiting for body',
                    )
                    return

                try:
                    networkidle_timeout_ms = int(self.request_timeout_ms / 3)
                    with self._monitored_step(
                        'wait_for_networkidle',
                        timeout_ms=networkidle_timeout_ms,
                    ):
                        page_playwright.wait_for_load_state(
                            'networkidle', timeout=networkidle_timeout_ms
                        )
                except PlaywrightTimeout:
                    span.set_attribute('playwright.networkidle.timeout', True)

                self.end_timer('goto.block')
                self.start_timer('goto.processing', count_towards_total=True)

                if status_code is not None and status_code >= 400:
                    metric_requests_failed_status_code.add(1, {'service': 'miner'})
                    self._halt(
                        span,
                        page_status=PageStatus.TODO,
                        reason=f'wrong http status code {status_code}',
                    )
                    return None

                text_content = fallback_text_content
                if text_content is None:
                    text_content = self._read_body_text(page_playwright, span)

                html_content = fallback_html_content
                if html_content is None:
                    html_content = self._read_html_content(page_playwright, span)
                title = self._read_page_title(page_playwright, span)

                if fallback_html_content is None:
                    with self._monitored_step('extract_dom_hrefs'):
                        anchors = page_playwright.locator('a[href]')
                        hrefs = anchors.evaluate_all('elements => elements.map(e => e.href)')
                    hrefs = self.filter_valids_hrefs(hrefs)
                else:
                    hrefs = self._extract_hrefs_from_html(fallback_html_content, final_url)
                    span.set_attribute('page.href_source', 'http_fallback_html')

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
                span.set_attribute('page.status_after', new_status.value)
                self.end_timer('page.save_results')

                if new_status == PageStatus.BLOCKED_LANGUAGE:
                    self._halt(
                        span,
                        reason='Wrong language detected',
                    )
                    return

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

                self._log_info('Saved new URLs', total_urls_saved=total_urls_saved)
                self._log_outcome(
                    'URL completed successfully',
                    page_status=PageStatus.DONE,
                    reason='done',
                    outcome='completed',
                    status_code=status_code,
                    final_url=final_url,
                    total_hrefs=len(hrefs),
                    total_urls_saved=total_urls_saved,
                    duplicate=self.pager.page.same_as is not None,
                    same_as=self.pager.page.same_as,
                )
                span.set_status(Status(StatusCode.OK))

                self.end_timer('request.begin')
                metric_request_duration.record(
                    self.get_timer_duration('request.begin'),
                    {'service': 'miner'},
                )

                return True

            except PlaywrightTimeout:
                self._halt(span, page_status=PageStatus.TODO, reason='Timeout error3')
                return None

            except (TargetClosedError, PlaywrightError) as e:
                if self.shutdown_event and self.shutdown_event.is_set():
                    self._halt(
                        span,
                        page_status=PageStatus.TODO,
                        reason='Shutdown closed playwright',
                        exc=e,
                    )
                    return None
                self._halt(
                    span,
                    page_status=PageStatus.TODO,
                    reason='TargetClosedError, PlaywrightError',
                    exc=e,
                )
                return None

            except AttributeError as e:
                self._halt(
                    span,
                    page_status=PageStatus.TODO,
                    reason='AttributeError as e',
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
                    exc=e,
                )
                return None

            finally:
                pass

            self._halt(
                span,
                page_status=PageStatus.FAILED,
                reason='Generic Exception',
            )
            return None
