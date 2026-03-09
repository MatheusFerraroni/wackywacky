import logging
import random
import signal
import time
from datetime import datetime
import json
from miner.db import get_connection
from miner.enums.system_status import SystemStatus
from miner.starter.starter import Starter
from miner.pager.pager import Pager
from miner.models.page import Page
from miner.requester import Requester
from miner.settings.settings_db import SettingsDB
from miner.settings.settings import settings
from opentelemetry import trace

from miner.metrics import (
    metric_pages_released_total,
    metric_any_request_duration_ms,
    metric_clean_db_duration_ms,
)

tracer = trace.get_tracer(__name__)


# TODO: add threads
class App:
    def __init__(self) -> None:
        self.logger = logging.getLogger(self.__class__.__name__)
        self._running = True
        self.conn = get_connection()
        self.last_clean_db = None

        self.retry_loop = 0
        self.max_retry_loop = 10

    def _handle_shutdown_signal(self, signum: int, _frame) -> None:
        self.logger.info('Starting graceful shutdown (signal=%s)', signum)
        self._running = False

    def _install_signal_handlers(self) -> None:
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)

    def get_system_status(self) -> SystemStatus:
        with tracer.start_as_current_span("app.get_system_status") as span:
            raw = SettingsDB().get_config('system_status', refresh=True)

            if raw is None:
                return SystemStatus.ERROR

            if isinstance(raw, SystemStatus):
                return raw

            if isinstance(raw, str):
                try:
                    return SystemStatus(raw.lower())
                except ValueError as e:
                    return SystemStatus.ERROR

            return SystemStatus.ERROR

    def set_system_status(self, status):
        with tracer.start_as_current_span("app.set_system_status") as span:
            if isinstance(status, SystemStatus):
                normalized = status.value
            else:
                raise Exception('Can only use SystemStatus type')

            json_value = json.dumps(normalized)

            with self.conn.cursor() as cursor:
                cursor.execute(
                    """
                    INSERT INTO settings (`key`, value)
                    VALUES (%s, CAST(%s AS JSON))
                    ON DUPLICATE KEY UPDATE value = CAST(%s AS JSON)
                    """,
                    ("system_status", json_value, json_value),
                )
            self.conn.commit()

    def run(self) -> int:
        with tracer.start_as_current_span("app.run") as span:
            self.logger.info('Starting run()')
            self._install_signal_handlers()

            try:
                while self._running:
                    should_clean_db = False
                    if not self.last_clean_db:
                        should_clean_db = True
                    else:
                        total_seconds_since_last_clean = ( datetime.now() - self.last_clean_db).total_seconds()
                        if total_seconds_since_last_clean > settings.SECONDS_BETWEEN_CLEAN_DB:
                            should_clean_db = True
                    
                    if should_clean_db:
                        self.last_clean_db = datetime.now()
                        self.clean_db()

                    self.system_status = self.get_system_status()
                    span.set_attribute("system.status", self.system_status.value)
                    self.retry_loop += 1
                    span.set_attribute("retry_loop", self.retry_loop)

                    match self.system_status:
                        case SystemStatus.STARTING:
                            self.logger.info(f'System is starting')
                            self.init_starter()
                        case SystemStatus.RUNNING_STARTER:
                            self.logger.info(f'Waiting to start')
                            time.sleep(1) # just wait until it's ready to mine
                        case SystemStatus.RUNNING_MINING:
                            mine_result = self.mine()
                            if mine_result:
                                self.retry_loop = 0
                        case SystemStatus.COMPLETED:
                            self.logger.info(f'System status is {SystemStatus.COMPLETED}. Quitting')
                        case SystemStatus.ERROR:
                            self.logger.error('Error. Quitting')
                        case _:
                            self.logger.info('Default system status. Quiting.')
                            self._running = False

                    if self.retry_loop >= self.max_retry_loop:
                        span.add_event("max_retry_reached")
                        self._running = False
                return 0

            except Exception as exc:
                span.record_exception(exc)
                span.set_status(trace.Status(trace.StatusCode.ERROR, str(exc)))
                self.logger.exception('Failed to execute')
                return 1
            finally:
                self.logger.info('Graceful shutdown completed')

    def init_starter(self):
        with tracer.start_as_current_span("app.init_starter") as span:
            self.set_system_status(SystemStatus.RUNNING_STARTER)
            starter = Starter()
            init_urls = starter.get_init_urls()
            span.set_attribute("starter.init_urls.count", len(init_urls))

            for init_url in init_urls:
                page = Pager(init_url)
                page.save()

            self.set_system_status(SystemStatus.RUNNING_MINING)

    def clean_db(self):
        with tracer.start_as_current_span("app.clean_db") as span:
            start_timer = time.perf_counter()
            self.logger.info('Cleaning DB')
            total = Page.release_stucked_processing()
            metric_pages_released_total.add(total, {'service': 'miner'})
            span.set_attribute("db.pages_released", total)
            self.logger.info(f'Released {total} pages')
            metric_clean_db_duration_ms.record(
                (time.perf_counter() - start_timer) * 1000,
                {"service": "miner"}
            )


    def mine(self):
        with tracer.start_as_current_span("app.mine") as span:
            self.logger.info('Miner started')
            url = Page.claim_next_todo_url()

            if url is None:
                span.add_event("nothing_to_mine")
                self.logger.warning('Nothing to mine')
                return False

            span.set_attribute("page.url", url)
            pager = Pager(url)
            pager.load()

            requester = Requester()
            requester.prepare(pager)
            with tracer.start_as_current_span("app.requesting") as span_requester:
                start_timer = time.perf_counter()
                requester.request()
                metric_any_request_duration_ms.record(
                    (time.perf_counter() - start_timer) * 1000,
                    {"service": "miner"}
                )
            return True
