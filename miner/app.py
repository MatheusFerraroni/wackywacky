import logging
import random
import signal
import time
from datetime import datetime
import json
import secrets
from miner.db import get_connection, close_connection
from miner.enums.system_status import SystemStatus
from miner.starter.starter import Starter
from miner.pager.pager import Pager
from miner.requester import Requester
from miner.settings.settings_db import SettingsDB
from miner.settings.settings import settings
from miner.leader import LeaderElection
import threading
from miner.enums import PageStatus
from contextlib import suppress
from miner.models import Domain, Page, PageComplete

from miner.metrics import (
    metric_pages_released,
    metric_any_request_duration,
    metric_clean_db_duration,
    metric_threads_alive,
)

from playwright.sync_api import sync_playwright


class App:
    def __init__(self, reset_db=False) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self._running = True

        self.retry_loop = 0
        self.max_retry_loop = 10
        self.threads = []
        self.threads_lock = threading.RLock()
        self.shutdown_event = threading.Event()
        self.lock_claim_url = threading.RLock()
        self.worker_id = 0

        self._last_threads_alive = 0

        self.leader = LeaderElection()
        self._leader_checked = False

        self.should_reset_db = reset_db

        self.last_system_status = None

        self.last_execution_timers = {
            'last_system_status_time': None,
            'last_clean_db': datetime.now(),
            'last_log_threads': datetime.now(),
            'last_move_complete_pages': datetime.now(),
        }

    def check_timers_executions(self):
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

        should_move_complete_pages = self.last_execution_timers['last_move_complete_pages'] is None
        if not should_move_complete_pages:
            total_seconds = (
                datetime.now() - self.last_execution_timers['last_move_complete_pages']
            ).total_seconds()
            if total_seconds > settings.SECONDS_BETWEEN_MOVE_COMPLETE_PAGES:
                should_move_complete_pages = True
        if should_move_complete_pages and self.leader.is_leader:
            self.last_execution_timers['last_move_complete_pages'] = datetime.now()
            total_moved = PageComplete.transfer_to_complete()

            self.logger.info('Moved %s completed pages', total_moved)

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
        self.logger.info('Cleaning DB started')
        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute('SET FOREIGN_KEY_CHECKS = 0')
            try:
                cursor.execute('TRUNCATE TABLE pages')
                cursor.execute('TRUNCATE TABLE domain')
                cursor.execute('TRUNCATE TABLE pages_complete')
                cursor.execute('TRUNCATE TABLE cache_url')
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

    def get_system_status(self) -> SystemStatus:

        if self.shutdown_event.is_set():
            return SystemStatus.STOPPING

        should_update_system_status = False

        if (
            self.last_system_status != SystemStatus.STARTING
            and self.last_system_status != SystemStatus.RUNNING_STARTER
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
        if not self.leader or not self.leader.is_leader:
            return

        if isinstance(status, SystemStatus):
            normalized = status.value
        else:
            raise Exception('Can only use SystemStatus type')

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

    def run(self) -> int:
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
                        self.logger.info(f'System status is {SystemStatus.COMPLETED}. Quitting')
                        self._running = False
                    case SystemStatus.STOPPING:
                        self.logger.info(f'System status is {SystemStatus.STOPPING}. Quitting')
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

        except Exception as e:
            self.logger.exception('Failed to execute')
            self.logger.exception(e)
            return 1
        finally:
            self.logger.info('Waiting miner threads to finish')
            with self.threads_lock:
                threads_snapshot = list(self.threads)
            for t in threads_snapshot:
                t.join(timeout=1)
            close_connection()
            self.logger.info('Graceful shutdown completed')

    def init_starter(self):
        self.set_system_status(SystemStatus.RUNNING_STARTER)
        starter = Starter()
        init_urls = starter.get_init_urls()

        for init_url in init_urls:
            domain = Domain.get_or_create(init_url)
            Page.get_or_create(domain_id=domain.id, url=init_url)

        self.set_system_status(SystemStatus.RUNNING_MINING)

    def clean_db(self):
        start_timer = time.perf_counter()
        self.logger.info('Releasing stucked pages DB')
        total = Page.release_stucked_processing()
        metric_pages_released.add(total, {'service': 'miner', 'leader': self.leader.is_leader})
        self.logger.info(f'Released {total} pages')
        metric_clean_db_duration.record(
            (time.perf_counter() - start_timer),
            {'service': 'miner', 'leader': self.leader.is_leader},
        )

    def generate_worker_name(self):
        self.worker_id += 1
        timestamp = datetime.utcnow().strftime('%Y%m%d%H%M%S')
        hash6 = secrets.token_hex(3)
        return f'miner-{self.worker_id}-{timestamp}-{hash6}'

    def mine(self) -> bool:
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
        user_agent = (
            'Mozilla/5.0 (X11; Linux x86_64) '
            'AppleWebKit/537.36 (KHTML, like Gecko) '
            'Chrome/122.0.0.0 Safari/537.36'
        )

        return browser.new_context(
            user_agent=user_agent,
            java_script_enabled=True,
            ignore_https_errors=True,
            viewport={'width': 1366, 'height': 768},
        )

    def _block_unneeded_resources(self, page):
        def route_handler(route):
            try:
                resource_type = route.request.resource_type
                if resource_type in {'image', 'media', 'font'}:
                    route.abort()
                else:
                    route.continue_()
            except Exception:
                return

        page.route('**/*', route_handler)

    def _mine(self):
        thread_name = threading.current_thread().name
        self.logger.info('Miner worker started | thread=%s', thread_name)

        page = None
        context = None
        browser = None

        recreate_browser_every = 200
        processed = recreate_browser_every + 1

        try:
            with sync_playwright() as pw:
                try:
                    while not self.shutdown_event.is_set():
                        if processed >= recreate_browser_every:
                            processed = 0
                            if page is not None:
                                with suppress(Exception):
                                    page.close()
                                page = None

                            if context is not None:
                                with suppress(Exception):
                                    context.close()
                                context = None

                            if browser is not None:
                                with suppress(Exception):
                                    browser.close()
                                browser = None
                            browser = pw.chromium.launch(
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
                        url = None

                        while not self.shutdown_event.is_set():
                            with self.lock_claim_url:
                                url = Page.claim_next_todo_url()

                            if url is not None:
                                break
                            time.sleep(random.random() * 0.5)

                        if self.shutdown_event.is_set():
                            return

                        if url is None:
                            self.logger.warning('Nothing to mine')
                            return

                        pager = Pager(url)
                        pager.load()

                        if self.shutdown_event.is_set():
                            pager.page.update(status=PageStatus.TODO)
                            self.logger.info('Detected shutdown event')
                            return

                        requester = Requester(shutdown_event=self.shutdown_event)
                        requester.prepare(pager)

                        start_timer = time.perf_counter()
                        try:
                            processed += 1
                            requester.request(page)
                        except Exception:
                            self.logger.exception('Unhandled exception inside requester')
                        metric_any_request_duration.record(
                            (time.perf_counter() - start_timer),
                            {'service': 'miner', 'leader': self.leader.is_leader},
                        )

                except Exception as e:
                    self.logger.exception('Unhandled exception in miner thread')
                    raise e

        finally:
            if page is not None:
                with suppress(Exception):
                    page.close()
            if context is not None:
                with suppress(Exception):
                    context.close()
            if browser is not None:
                with suppress(Exception):
                    browser.close()
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
