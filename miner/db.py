import logging
import threading
import time
import pymysql
from miner.settings.settings import settings

logger = logging.getLogger('db')
_thread_local = threading.local()


def _create_connection(max_wait_seconds=150, retry_interval=2):
    logger.info(
        'Creating DB connection (host=%s, port=%s, db=%s)',
        settings.DB_HOST,
        settings.DB_PORT,
        settings.DB_NAME,
    )

    deadline = time.monotonic() + max_wait_seconds
    last_error = None

    while time.monotonic() < deadline:
        try:
            conn = pymysql.connect(
                host=settings.DB_HOST,
                port=settings.DB_PORT,
                user=settings.DB_USER,
                password=settings.DB_PASSWORD,
                database=settings.DB_NAME,
                cursorclass=pymysql.cursors.DictCursor,
                autocommit=False,
            )
            logger.info('DB connection established successfully')
            return conn

        except pymysql.MySQLError as exc:
            last_error = exc
            logger.warning(
                'DB not ready yet. Retrying in %ss... (%s)',
                retry_interval,
                exc,
            )
            time.sleep(retry_interval)
            retry_interval = max(10, retry_interval + 1)

    raise RuntimeError(f'Could not connect to DB after {max_wait_seconds}s') from last_error


def get_connection():
    conn = getattr(_thread_local, 'connection', None)

    if conn is not None:
        try:
            conn.ping(reconnect=True)
            return conn
        except Exception:
            logger.warning('Thread DB connection lost. Reconnecting...')
            try:
                conn.close()
            except Exception:
                pass

    conn = _create_connection()
    _thread_local.connection = conn
    return conn


def close_connection():
    conn = getattr(_thread_local, 'connection', None)
    if conn is not None:
        try:
            conn.close()
        finally:
            _thread_local.connection = None


def test_connection():
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute('SELECT 1 as test')
        return cursor.fetchone()
