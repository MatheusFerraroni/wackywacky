import logging
import threading
import pymysql
from miner.settings.settings import settings

logger = logging.getLogger("db")
_thread_local = threading.local()


def _create_connection():
    logger.info(
        "Creating DB connection (host=%s, port=%s, db=%s)",
        settings.DB_HOST,
        settings.DB_PORT,
        settings.DB_NAME,
    )
    return pymysql.connect(
        host=settings.DB_HOST,
        port=settings.DB_PORT,
        user=settings.DB_USER,
        password=settings.DB_PASSWORD,
        database=settings.DB_NAME,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=True,
    )


def get_connection():
    conn = getattr(_thread_local, "connection", None)

    if conn is not None:
        try:
            conn.ping(reconnect=True)
            return conn
        except Exception:
            logger.warning("Thread DB connection lost. Reconnecting...")
            try:
                conn.close()
            except Exception:
                pass

    conn = _create_connection()
    _thread_local.connection = conn
    return conn


def close_connection():
    conn = getattr(_thread_local, "connection", None)
    if conn is not None:
        try:
            conn.close()
        finally:
            _thread_local.connection = None


def test_connection():
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1 as test")
        return cursor.fetchone()
