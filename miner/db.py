import logging
import pymysql
from miner.settings.settings import settings

logger = logging.getLogger("db")

_connection = None

def get_connection():
    global _connection

    if _connection is not None:
        try:
            _connection.ping(reconnect=True)
            logger.debug('Returning db connection from singleton')
            return _connection
        except Exception:
            logger.warning("Connection lost. Reconnecting...")

    logger.info(
        "Creating DB connection (host=%s, port=%s, db=%s)",
        settings.DB_HOST,
        settings.DB_PORT,
        settings.DB_NAME,
    )

    try:
        _connection = pymysql.connect(
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
            database=settings.DB_NAME,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )
        logger.info("Database connection established")
        return _connection

    except Exception:
        logger.exception("Failed to establish database connection")
        raise

def test_connection():
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute("SELECT 1 as test")
        result = cursor.fetchone()
    conn.close()
    return result