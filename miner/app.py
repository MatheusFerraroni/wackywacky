"""Application runtime loop and worker orchestration."""

import asyncio
import json
import logging
import multiprocessing
import random
import secrets
import signal
import threading
import time
from contextlib import suppress
from datetime import datetime
from urllib.error import URLError
from urllib.parse import urljoin, urlparse, urlunparse
from urllib.request import urlopen

from playwright.sync_api import Error as PlaywrightError
from playwright.sync_api import sync_playwright

from miner.db import close_connection, get_connection
from miner.enums import PageStatus
from miner.enums.system_status import SystemStatus
from miner.leader import LeaderElection
from miner.metrics import (
    metric_any_request_duration,
    metric_clean_db_duration,
    metric_page_hard_timeouts,
    metric_pages_released,
    metric_threads_alive,
)
from miner.models import Domain, Page
from miner.pager.pager import Pager
from miner.requester import Requester
from miner.settings.settings import settings
from miner.settings.settings_db import SettingsDB
from miner.starter.starter import Starter


def _run_page_request_subprocess(page_id: int) -> None:
    """Process exactly one page in an isolated child process."""
    # Import inside the child so spawned processes configure logging like the main entrypoint.
    # pylint: disable=import-outside-toplevel
    from miner.logging_config import configure_logging
    from miner.telemetry import setup_telemetry

    configure_logging(level=logging.INFO)
    setup_telemetry()

    logger = logging.getLogger('PageProcess')
    child_shutdown_event = threading.Event()
    child_app = App()
    browser = None
    context = None
    page = None

    try:
        pager = Pager(page_id)
        pager.load()
        logger.info(
            'Page subprocess loaded page | page_id=%s domain_id=%s url=%s',
            pager.page.id,
            pager.domain.id,
            pager.page.url,
        )

        with sync_playwright() as pw:
            browser = child_app._build_browser(pw)  # pylint: disable=protected-access
            context = child_app._build_context(browser)  # pylint: disable=protected-access
            page = context.new_page()
            child_app._block_unneeded_resources(page)  # pylint: disable=protected-access

            requester = Requester(shutdown_event=child_shutdown_event)
            requester.prepare(pager)
            requester.request(page)
    finally:
        if page is not None:
            with suppress(asyncio.CancelledError, Exception):
                page.unroute('**/*')
            with suppress(asyncio.CancelledError, Exception):
                page.close()
        if context is not None:
            with suppress(asyncio.CancelledError, Exception):
                context.close()
        if browser is not None:
            with suppress(asyncio.CancelledError, Exception):
                browser.close()
        close_connection()
        logger.info('Page subprocess finished cleanup | page_id=%s', page_id)


class App:  # pylint: disable=too-many-instance-attributes
    """Coordinate leader tasks, worker threads, and browser lifecycle."""

    def __init__(self, reset_db=False) -> None:
        """Initialize the application state."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self._running = True

        self.retry_loop = 0
        self.max_retry_loop = 10
        self.threads = []
        self.threads_lock = threading.RLock()
        self.shutdown_event = threading.Event()
        self.lock_claim_url = threading.RLock()
        self.child_processes = {}
        self.child_processes_lock = threading.RLock()
        self.worker_id = 0

        self._last_threads_alive = 0

        self.leader = LeaderElection()
        self._leader_checked = False

        self.should_reset_db = reset_db

        self.last_system_status = None
        self.system_status = None

        self.last_execution_timers = {
            'last_system_status_time': None,
            'last_clean_db': datetime.now(),
            'last_log_threads': datetime.now(),
        }

    def check_timers_executions(self):
        """Run periodic cleanup and thread logging tasks."""
        should_clean_db = self.last_execution_timers['last_clean_db'] is None
        if not should_clean_db:
            total_seconds = (
                datetime.now() - self.last_execution_timers['last_clean_db']
            ).total_seconds()
            if total_seconds > settings.SECONDS_BETWEEN_CLEAN_DB:
                should_clean_db = True
        if should_clean_db and self.leader.is_leader:
            self.last_execution_timers['last_clean_db'] = datetime.now()
            self.clean_db()

        should_log_threads = False
        if not self.last_execution_timers['last_log_threads']:
            should_log_threads = True
        else:
            total_seconds = (
                datetime.now() - self.last_execution_timers['last_log_threads']
            ).total_seconds()
            if total_seconds > settings.SECONDS_BETWEEN_LOG_THREADS:
                should_log_threads = True
        if should_log_threads:
            self.last_execution_timers['last_log_threads'] = datetime.now()
            self._log_thread_stats()

    def reset_db(self):
        """Reset crawl tables and return the system to the starting state."""
        self.logger.info('Cleaning DB started')
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute('SET FOREIGN_KEY_CHECKS = 0')
            try:
                cursor.execute('TRUNCATE TABLE pages')
                cursor.execute('TRUNCATE TABLE domain')
                cursor.execute(
                    """
                    UPDATE settings
                    SET value = %s
                    WHERE `key` = %s
                    """,
                    ('"starting"', 'system_status'),
                )
            finally:
                cursor.execute('SET FOREIGN_KEY_CHECKS = 1')
        conn.commit()

        with conn.cursor() as cursor:
            cursor.execute('SELECT COUNT(1) AS total FROM pages')
            pages_count = cursor.fetchone()['total']

            cursor.execute('SELECT COUNT(1) AS total FROM domain')
            domain_count = cursor.fetchone()['total']

        if pages_count != 0 or domain_count != 0:
            raise RuntimeError(
                f'Database reset validation failed: pages={pages_count}, domain={domain_count}'
            )

        self.logger.info('Cleaning DB completed')

    def _handle_shutdown_signal(self, signum: int, _frame) -> None:
        self.logger.info('Starting graceful shutdown (signal=%s)', signum)
        self._running = False
        self.shutdown_event.set()

    def _install_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)

    def get_system_status(self) -> SystemStatus:  # pylint: disable=too-many-return-statements
        """Read and cache the current system status."""

        if self.shutdown_event.is_set():
            return SystemStatus.STOPPING

        should_update_system_status = False

        if self.last_system_status not in (
            SystemStatus.STARTING,
            SystemStatus.RUNNING_STARTER,
        ):
            if self.last_execution_timers['last_system_status_time'] is not None:
                total_seconds = (
                    datetime.now() - self.last_execution_timers['last_system_status_time']
                ).total_seconds()
                if total_seconds > settings.SECONDS_BETWEEN_UPDATE_SYSTEM_STATUS:
                    should_update_system_status = True
            else:
                should_update_system_status = True

            if not should_update_system_status:
                return self.last_system_status

        self.last_execution_timers['last_system_status_time'] = datetime.now()
        raw = SettingsDB().get_config('system_status', refresh=True)

        if raw is None:
            self.last_system_status = SystemStatus.ERROR
            return self.last_system_status

        if isinstance(raw, SystemStatus):
            self.last_system_status = raw
            return self.last_system_status

        if isinstance(raw, str):
            try:
                self.last_system_status = SystemStatus(raw.lower())
                return self.last_system_status
            except ValueError:
                self.last_system_status = SystemStatus.ERROR
                return self.last_system_status

        self.last_system_status = SystemStatus.ERROR
        return self.last_system_status

    def set_system_status(self, status):
        """Persist a new system status when this process is the leader."""
        if not self.leader or not self.leader.is_leader:
            return

        if isinstance(status, SystemStatus):
            normalized = status.value
        else:
            raise TypeError('Can only use SystemStatus type')

        json_value = json.dumps(normalized)

        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO settings (`key`, value)
                VALUES (%s, CAST(%s AS JSON))
                ON DUPLICATE KEY UPDATE value = CAST(%s AS JSON)
                """,
                ('system_status', json_value, json_value),
            )
        conn.commit()

    def run(self) -> int:  # pylint: disable=too-many-branches,too-many-statements,broad-exception-caught
        """Run the main application loop until shutdown or completion."""
        self.logger.info('Starting run()')
        self._install_signal_handlers()

        try:
            became_leader = self.leader.acquire(timeout_seconds=0)
            if became_leader:
                self.logger.info('This instance is the global leader')
            else:
                self.logger.info('This instance is a worker')

            if self.should_reset_db and self.leader.is_leader:
                self.reset_db()

            while self._running:
                self.leader.refresh()

                self.check_timers_executions()

                self.system_status = self.get_system_status()
                self.retry_loop += 1

                match self.system_status:
                    case SystemStatus.STARTING:
                        if self.leader.is_leader:
                            self.logger.info('System is starting')
                            self.init_starter()
                        else:
                            self.logger.info('Waiting for leader to run init_starter')
                            time.sleep(1)
                    case SystemStatus.RUNNING_STARTER:
                        self.logger.info('Waiting leader to finish init_starter')
                        time.sleep(1)  # just wait until it's ready to mine
                    case SystemStatus.RUNNING_MINING:
                        started = self.mine()

                        with self.threads_lock:  # prevent early stop with long running threads
                            if len(self.threads) > 0:
                                time.sleep(0.5)
                                continue

                        if started:
                            self.retry_loop = 0
                        else:
                            time.sleep(0.5)
                    case SystemStatus.COMPLETED:
                        self.logger.info('System status is %s. Quitting', SystemStatus.COMPLETED)
                        self._running = False
                    case SystemStatus.STOPPING:
                        self.logger.info('System status is %s. Quitting', SystemStatus.STOPPING)
                        self._running = False
                    case SystemStatus.ERROR:
                        self.logger.error('Error. Quitting')
                        self._running = False
                    case _:
                        self.logger.info('Default system status. Quiting.')
                        self._running = False

                if self.retry_loop >= self.max_retry_loop:
                    self._running = False
            return 0

        # The main loop boundary must convert any unexpected failure into a process exit code.
        # pylint: disable-next=broad-exception-caught
        except Exception as e:
            self.logger.exception('Failed to execute')
            self.logger.exception(e)
            return 1
        finally:
            shutdown_completed = self._wait_miner_threads_to_finish()
            close_connection()
            if shutdown_completed:
                self.logger.info('Graceful shutdown completed')
            else:
                self.logger.warning('Graceful shutdown finished with miner threads still alive')

    def _wait_miner_threads_to_finish(self) -> bool:
        """Wait for miner threads to finish before declaring shutdown complete."""
        timeout_seconds = settings.GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS
        deadline = time.monotonic() + timeout_seconds

        self.logger.info(
            'Waiting miner threads to finish (timeout=%ss)',
            timeout_seconds,
        )

        while True:
            if self.shutdown_event.is_set():
                self._terminate_active_page_processes('graceful_shutdown')

            with self.threads_lock:
                alive_threads = [thread for thread in self.threads if thread.is_alive()]
                self.threads = alive_threads

            if not alive_threads:
                return True

            remaining_seconds = deadline - time.monotonic()
            if remaining_seconds <= 0:
                self.logger.warning(
                    'Graceful shutdown timeout reached with alive miner threads: %s',
                    [thread.name for thread in alive_threads],
                )
                return False

            for thread in alive_threads:
                thread.join(timeout=min(1, max(0.1, remaining_seconds)))

    def init_starter(self):
        """Seed the first URLs and switch the system to mining."""
        self.set_system_status(SystemStatus.RUNNING_STARTER)
        starter = Starter()
        init_urls = starter.get_init_urls()

        for init_url in init_urls:
            domain = Domain.get_or_create(init_url)
            Page.get_or_create(domain_id=domain.id, url=init_url)

        self.set_system_status(SystemStatus.RUNNING_MINING)

    def clean_db(self):
        """Release processing pages that exceeded their lease timeout."""
        start_timer = time.perf_counter()
        self.logger.info('Releasing stucked pages DB')
        total = Page.release_stucked_processing()
        metric_pages_released.add(total, {'service': 'miner', 'leader': self.leader.is_leader})
        self.logger.info('Released %s pages', total)
        metric_clean_db_duration.record(
            (time.perf_counter() - start_timer),
            {'service': 'miner', 'leader': self.leader.is_leader},
        )

    def generate_worker_name(self):
        """Generate a readable unique worker thread name."""
        self.worker_id += 1
        timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        hash6 = secrets.token_hex(3)
        return f'miner-{self.worker_id}-{timestamp}-{hash6}'

    def mine(self) -> bool:
        """Start worker threads until the configured concurrency is reached."""
        started_any = False
        max_threads = settings.MAX_THREADS

        with self.threads_lock:
            alive_threads = []
            for t in self.threads:
                if t.is_alive():
                    alive_threads.append(t)
            self.threads = alive_threads

            available_slots = max_threads - len(self.threads)

            if available_slots <= 0:
                return False

            for _ in range(available_slots):
                time.sleep(random.random())  # random sleep to reduce concorrence at start
                t = threading.Thread(target=self._mine, name=self.generate_worker_name())
                t.start()
                self.threads.append(t)
                started_any = True

            return started_any

    def _build_context(self, browser):
        """Create a browser context with crawler defaults."""
        user_agent = (
            'Mozilla/5.0 (X11; Linux x86_64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/122.0.0.0 Safari/537.36'
        )

        context = browser.new_context(
            user_agent=user_agent,
            java_script_enabled=True,
            ignore_https_errors=True,
            viewport={'width': 1366, 'height': 768},
        )
        context.set_default_timeout(settings.PLAYWRIGHT_DEFAULT_TIMEOUT_MS)
        context.set_default_navigation_timeout(settings.PLAYWRIGHT_DEFAULT_TIMEOUT_MS)
        return context

    def _build_browser(self, pw):
        """Create or connect to the configured browser backend."""
        if settings.BROWSER_BACKEND == 'obscura':
            return self._connect_obscura_browser(pw)

        if settings.BROWSER_BACKEND == 'chromium':
            return pw.chromium.launch(
                headless=True,
                args=[
                    '--disable-blink-features=AutomationControlled',
                    '--no-sandbox',
                    '--disable-dev-shm-usage',
                ],
            )

        raise ValueError(f'Unsupported BROWSER_BACKEND: {settings.BROWSER_BACKEND}')

    def _connect_obscura_browser(self, pw):
        """Connect to Obscura, waiting until the CDP endpoint is ready."""
        deadline = time.monotonic() + settings.OBSCURA_CDP_CONNECT_TIMEOUT_SECONDS
        attempt = 0
        last_error = None

        while not self.shutdown_event.is_set():
            attempt += 1
            try:
                endpoint = self._resolve_obscura_cdp_endpoint(settings.OBSCURA_CDP_ENDPOINT)
                self.logger.info(
                    'Connecting to Obscura CDP endpoint: %s',
                    endpoint,
                )
                return pw.chromium.connect_over_cdp(endpoint)
            except (OSError, URLError, PlaywrightError) as exc:
                last_error = exc
                remaining_seconds = deadline - time.monotonic()
                if remaining_seconds <= 0:
                    break

                sleep_seconds = min(2, max(0.2, remaining_seconds))
                self.logger.warning(
                    'Obscura CDP endpoint is not ready yet. Retrying in %.1fs '
                    '(attempt=%s, endpoint=%s, error=%s)',
                    sleep_seconds,
                    attempt,
                    settings.OBSCURA_CDP_ENDPOINT,
                    exc,
                )
                time.sleep(sleep_seconds)

        raise RuntimeError(
            'Could not connect to Obscura CDP endpoint '
            f'{settings.OBSCURA_CDP_ENDPOINT} after '
            f'{settings.OBSCURA_CDP_CONNECT_TIMEOUT_SECONDS}s'
        ) from last_error

    def _resolve_obscura_cdp_endpoint(self, endpoint: str) -> str:
        """Resolve Obscura's CDP WebSocket URL and keep it reachable from this container."""
        parsed_endpoint = urlparse(endpoint)
        if parsed_endpoint.scheme in {'ws', 'wss'}:
            return endpoint

        version_url = urljoin(endpoint.rstrip('/') + '/', 'json/version')
        try:
            with urlopen(version_url, timeout=5) as response:  # nosec B310
                payload = json.loads(response.read().decode('utf-8'))
        except (OSError, URLError, json.JSONDecodeError) as exc:
            self.logger.warning(
                'Could not resolve Obscura websocket URL from %s. Falling back to HTTP CDP. '
                'error=%s',
                version_url,
                exc,
            )
            return endpoint

        websocket_url = payload.get('webSocketDebuggerUrl')
        if not websocket_url:
            self.logger.warning(
                'Obscura /json/version did not return webSocketDebuggerUrl. '
                'Falling back to HTTP CDP.'
            )
            return endpoint

        parsed_websocket = urlparse(websocket_url)
        if parsed_websocket.hostname in {'127.0.0.1', 'localhost', '0.0.0.0', '::1'}:
            websocket_url = urlunparse(parsed_websocket._replace(netloc=parsed_endpoint.netloc))
            self.logger.info(
                'Rewrote Obscura websocket host from %s to %s',
                parsed_websocket.netloc,
                parsed_endpoint.netloc,
            )

        return websocket_url

    def _block_unneeded_resources(self, page):
        """Block resources that are not needed for text extraction."""
        seen_resource_types = set()
        blocked_resource_types = set()
        route_summary_logged = False

        def log_route_summary():
            nonlocal route_summary_logged
            if route_summary_logged:
                return
            self.logger.info(
                'Browser route resource types | seen=%s blocked=%s backend=%s',
                sorted(seen_resource_types),
                sorted(blocked_resource_types),
                settings.BROWSER_BACKEND,
            )
            route_summary_logged = True

        def route_handler(route):
            nonlocal route_summary_logged
            try:
                if self.shutdown_event.is_set():
                    route.abort()
                    return

                resource_type = route.request.resource_type
                seen_resource_types.add(resource_type)
                if resource_type in {'image', 'media', 'font'}:
                    blocked_resource_types.add(resource_type)
                    route.abort()
                else:
                    route.continue_()

                if not route_summary_logged and (
                    len(seen_resource_types) >= 4 or blocked_resource_types
                ):
                    log_route_summary()
            except (asyncio.CancelledError, PlaywrightError) as exc:
                if not route_summary_logged:
                    self.logger.warning(
                        'Browser route handler failed. Continuing without blocking. error=%s',
                        exc,
                    )
                    route_summary_logged = True

        try:
            page.route('**/*', route_handler)
            summary_timer = threading.Timer(5.0, log_route_summary)
            summary_timer.daemon = True
            summary_timer.start()
        except PlaywrightError as exc:
            self.logger.warning(
                'Browser backend does not support request routing. Continuing without route block. '
                'error=%s',
                exc,
            )

    def _claim_next_pager(self, thread_name: str) -> Pager | None:
        """Claim the next page and load its page/domain records."""
        page_id = None
        claim_wait = {
            'started_at': time.perf_counter(),
            'last_log_at': time.perf_counter(),
        }

        while not self.shutdown_event.is_set():
            with self.lock_claim_url:
                page_id = Page.claim_next_todo_url()

            if page_id is not None:
                break

            now = time.perf_counter()
            if now - claim_wait['last_log_at'] >= settings.SECONDS_BETWEEN_LOG_THREADS:
                self.logger.info(
                    'Waiting for claimable page | thread=%s wait_s=%.1f '
                    'processing_timeout_s=%s',
                    thread_name,
                    now - claim_wait['started_at'],
                    settings.PROCESSING_TIMEOUT_SECONDS,
                )
                claim_wait['last_log_at'] = now
            time.sleep(random.random() * 0.5)

        if self.shutdown_event.is_set():
            return None

        if page_id is None:
            self.logger.warning('Nothing to mine')
            return None

        pager = Pager(page_id)
        pager.load()
        self.logger.info(
            'Worker claimed page | thread=%s page_id=%s domain_id=%s '
            'retry_count=%s url=%s',
            thread_name,
            pager.page.id,
            pager.domain.id,
            pager.page.retry_count,
            pager.page.url,
        )

        if self.shutdown_event.is_set():
            pager.page.update(status=PageStatus.TODO)
            self.logger.info('Detected shutdown event')
            return None

        return pager

    def _return_processing_page_to_todo(self, page_id: int, reason: str) -> None:
        """Return a page to TODO only if it is still marked as processing."""
        try:
            page = Page.get_by_id(page_id)
            if page is None:
                self.logger.warning(
                    'Could not return page to TODO because it no longer exists | '
                    'page_id=%s reason=%s',
                    page_id,
                    reason,
                )
                return

            if page.status != PageStatus.PROCESSING.value:
                self.logger.info(
                    'Skipping return to TODO because page already moved | '
                    'page_id=%s status=%s reason=%s',
                    page_id,
                    page.status,
                    reason,
                )
                return

            page.update(status=PageStatus.TODO)
            self.logger.warning(
                'Returned processing page to TODO | page_id=%s reason=%s',
                page_id,
                reason,
            )
        # pylint: disable-next=broad-exception-caught
        except Exception:
            self.logger.exception(
                'Failed to return processing page to TODO | page_id=%s reason=%s',
                page_id,
                reason,
            )

    @staticmethod
    def _terminate_page_process(process, timeout_seconds: float = 5.0) -> None:
        """Terminate a page process, then force kill it if it does not exit."""
        if not process.is_alive():
            return

        process.terminate()
        process.join(timeout=timeout_seconds)
        if process.is_alive():
            process.kill()
            process.join(timeout=timeout_seconds)

    def _terminate_active_page_processes(self, reason: str) -> None:
        """Terminate all active page subprocesses during shutdown."""
        with self.child_processes_lock:
            active_processes = list(self.child_processes.items())

        for page_id, process in active_processes:
            if not process.is_alive():
                continue
            self.logger.warning(
                'Terminating active page subprocess | page_id=%s pid=%s reason=%s',
                page_id,
                process.pid,
                reason,
            )
            self._terminate_page_process(process)

    def _run_request_in_subprocess(self, pager: Pager) -> None:
        """Run one page request in a subprocess supervised by a hard timeout."""
        started_at = time.perf_counter()
        timeout_seconds = settings.MINER_PAGE_HARD_TIMEOUT_SECONDS
        context = multiprocessing.get_context('spawn')
        process = context.Process(
            target=_run_page_request_subprocess,
            args=(pager.page.id,),
            name=f'{threading.current_thread().name}-page-{pager.page.id}',
        )

        process.start()
        with self.child_processes_lock:
            self.child_processes[pager.page.id] = process

        self.logger.info(
            'Page subprocess started | thread=%s page_id=%s domain_id=%s pid=%s '
            'hard_timeout_s=%s',
            threading.current_thread().name,
            pager.page.id,
            pager.domain.id,
            process.pid,
            timeout_seconds,
        )

        timed_out = False
        stopped_for_shutdown = False
        deadline = started_at + timeout_seconds

        try:
            while process.is_alive():
                if self.shutdown_event.is_set():
                    stopped_for_shutdown = True
                    self.logger.warning(
                        'Stopping page subprocess due to shutdown | page_id=%s pid=%s',
                        pager.page.id,
                        process.pid,
                    )
                    self._terminate_page_process(process)
                    break

                remaining_seconds = deadline - time.perf_counter()
                if remaining_seconds <= 0:
                    timed_out = True
                    self.logger.error(
                        'Page subprocess exceeded hard timeout | page_id=%s domain_id=%s '
                        'pid=%s hard_timeout_s=%s',
                        pager.page.id,
                        pager.domain.id,
                        process.pid,
                        timeout_seconds,
                    )
                    metric_page_hard_timeouts.add(1, {'service': 'miner'})
                    self._terminate_page_process(process)
                    break

                process.join(timeout=min(0.5, max(0.1, remaining_seconds)))

            duration_s = time.perf_counter() - started_at
            exitcode = process.exitcode

            if timed_out:
                self._return_processing_page_to_todo(pager.page.id, 'page_hard_timeout')
            elif stopped_for_shutdown:
                self._return_processing_page_to_todo(pager.page.id, 'shutdown')
            elif exitcode != 0:
                self.logger.error(
                    'Page subprocess failed | page_id=%s domain_id=%s pid=%s '
                    'exitcode=%s duration_s=%.3f',
                    pager.page.id,
                    pager.domain.id,
                    process.pid,
                    exitcode,
                    duration_s,
                )
                self._return_processing_page_to_todo(pager.page.id, 'page_subprocess_failed')
            else:
                self.logger.info(
                    'Page subprocess finished | page_id=%s domain_id=%s pid=%s '
                    'exitcode=%s duration_s=%.3f',
                    pager.page.id,
                    pager.domain.id,
                    process.pid,
                    exitcode,
                    duration_s,
                )

            metric_any_request_duration.record(
                duration_s,
                {'service': 'miner', 'leader': self.leader.is_leader},
            )
        finally:
            with self.child_processes_lock:
                self.child_processes.pop(pager.page.id, None)

    def _run_request_inline(self, pager: Pager, page) -> None:
        """Run one page request inside the current worker thread."""
        requester = Requester(shutdown_event=self.shutdown_event)
        requester.prepare(pager)

        start_timer = time.perf_counter()
        try:
            requester.request(page)
        # Keep this worker alive when one page/request fails unexpectedly.
        # pylint: disable-next=broad-exception-caught
        except Exception:
            self.logger.exception('Unhandled exception inside requester')
        metric_any_request_duration.record(
            (time.perf_counter() - start_timer),
            {'service': 'miner', 'leader': self.leader.is_leader},
        )

    def _mine_isolated(self, thread_name: str) -> None:
        """Run the worker loop using one supervised child process per page."""
        while not self.shutdown_event.is_set():
            pager = self._claim_next_pager(thread_name)
            if pager is None:
                return
            self._run_request_in_subprocess(pager)

    def _mine_inline(self, thread_name: str) -> None:
        """Run the legacy worker loop with a reusable browser page in this thread."""
        page = None
        context = None
        browser = None

        recreate_browser_every = 200
        processed = recreate_browser_every + 1

        try:
            with sync_playwright() as pw:
                while not self.shutdown_event.is_set():
                    if processed >= recreate_browser_every:
                        processed = 0
                        if page is not None:
                            with suppress(asyncio.CancelledError, Exception):
                                page.unroute('**/*')
                            with suppress(asyncio.CancelledError, Exception):
                                page.close()
                            page = None

                        if context is not None:
                            with suppress(asyncio.CancelledError, Exception):
                                context.close()
                            context = None

                        if browser is not None:
                            with suppress(asyncio.CancelledError, Exception):
                                browser.close()
                            browser = None

                        browser = self._build_browser(pw)
                        context = self._build_context(browser)
                        page = context.new_page()
                        self._block_unneeded_resources(page)

                    pager = self._claim_next_pager(thread_name)
                    if pager is None:
                        return

                    processed += 1
                    self._run_request_inline(pager, page)
        finally:
            if page is not None:
                with suppress(asyncio.CancelledError, Exception):
                    page.unroute('**/*')
                with suppress(asyncio.CancelledError, Exception):
                    page.close()
            if context is not None:
                with suppress(asyncio.CancelledError, Exception):
                    context.close()
            if browser is not None:
                with suppress(asyncio.CancelledError, Exception):
                    browser.close()

    def _mine(self):  # pylint: disable=broad-exception-caught
        """Run the worker loop for one mining thread."""
        thread_name = threading.current_thread().name
        self.logger.info(
            'Miner worker started | thread=%s page_isolation_enabled=%s '
            'page_hard_timeout_s=%s',
            thread_name,
            settings.MINER_PAGE_ISOLATION_ENABLED,
            settings.MINER_PAGE_HARD_TIMEOUT_SECONDS,
        )

        try:
            if settings.MINER_PAGE_ISOLATION_ENABLED:
                self._mine_isolated(thread_name)
            else:
                self._mine_inline(thread_name)
        except Exception as e:
            self.logger.exception('Unhandled exception in miner thread')
            raise e
        finally:
            close_connection()
            self.logger.info('Quitting Worker')

    def _log_thread_stats(self) -> None:
        with self.threads_lock:
            total = len(self.threads)
            alive = sum(1 for t in self.threads if t.is_alive())
            dead = total - alive

            thread_names = [t.name for t in self.threads if t.is_alive()]

        delta = alive - self._last_threads_alive
        metric_threads_alive.add(delta, {'service': 'miner'})
        self._last_threads_alive = alive

        self.logger.info(
            'Thread pool stats | total=%s alive=%s dead=%s threads=%s',
            total,
            alive,
            dead,
            thread_names,
        )
