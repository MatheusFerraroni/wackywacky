"""Reset crawler state in the configured MySQL database."""

import logging
from contextlib import suppress

from miner.db import close_connection, get_connection
from miner.leader import LeaderElection
from miner.settings.settings import settings


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
)
logger = logging.getLogger(__name__)


def _current_connection_id(cursor) -> int:
    """Return this script's MySQL connection id."""
    cursor.execute('SELECT CONNECTION_ID() AS connection_id')
    return int(cursor.fetchone()['connection_id'])


def _leader_lock_connection_id(cursor) -> int | None:
    """Return the connection id that owns the leader lock, when present."""
    cursor.execute('SELECT IS_USED_LOCK(%s) AS connection_id', (LeaderElection().lock_name,))
    row = cursor.fetchone()
    connection_id = row['connection_id'] if row else None
    return int(connection_id) if connection_id is not None else None


def _connection_ids_to_kill(cursor, current_connection_id: int) -> set[int]:
    """Return app database connection ids that should be closed before reset."""
    connection_ids = set()

    cursor.execute('SHOW PROCESSLIST')
    for row in cursor.fetchall():
        connection_id = int(row['Id'])
        if connection_id == current_connection_id:
            continue

        if row.get('User') != settings.DB_USER:
            continue

        if row.get('db') != settings.DB_NAME:
            continue

        connection_ids.add(connection_id)

    leader_connection_id = _leader_lock_connection_id(cursor)
    if leader_connection_id is not None and leader_connection_id != current_connection_id:
        connection_ids.add(leader_connection_id)

    return connection_ids


def _kill_connection(cursor, connection_id: int) -> None:
    """Kill a MySQL connection id after it has been normalized to int."""
    cursor.execute(f'KILL {int(connection_id)}')


def close_active_database_connections(cursor) -> None:
    """Close active app database connections and release the global leader lock."""
    current_connection_id = _current_connection_id(cursor)
    connection_ids = _connection_ids_to_kill(cursor, current_connection_id)

    if not connection_ids:
        logger.info('No active app database connections to close')
        return

    logger.info('Closing active app database connections: %s', sorted(connection_ids))
    for connection_id in sorted(connection_ids):
        _kill_connection(cursor, connection_id)

    leader_connection_id = _leader_lock_connection_id(cursor)
    if leader_connection_id is None:
        logger.info('Leader lock is free')
    else:
        logger.warning('Leader lock is still held by connection %s', leader_connection_id)


def ensure_pages_content_columns(cursor) -> None:
    """Ensure text/html storage can hold the configured crawler content size."""
    logger.info('Ensuring pages.text and pages.html are MEDIUMBLOB')
    cursor.execute(
        """
        ALTER TABLE pages
            MODIFY COLUMN `text` MEDIUMBLOB NULL,
            MODIFY COLUMN `html` MEDIUMBLOB NULL
        """
    )


def ensure_domain_cooldown_columns(cursor) -> None:
    """Ensure domain cooldown timestamps preserve subsecond precision."""
    logger.info('Ensuring domain cooldown timestamp columns use TIMESTAMP(6)')
    cursor.execute(
        """
        ALTER TABLE domain
            MODIFY COLUMN last_request_at TIMESTAMP(6) NULL DEFAULT NULL,
            MODIFY COLUMN updated_at TIMESTAMP(6) NOT NULL
                DEFAULT CURRENT_TIMESTAMP(6)
                ON UPDATE CURRENT_TIMESTAMP(6)
        """
    )


def reset_db():
    """Clear crawl tables and put system status back to starting."""
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            close_active_database_connections(cursor)
            ensure_pages_content_columns(cursor)
            ensure_domain_cooldown_columns(cursor)

            logger.info('Disabling foreign key checks')
            cursor.execute('SET FOREIGN_KEY_CHECKS = 0')

            logger.info('Truncating pages')
            cursor.execute('TRUNCATE TABLE pages')

            logger.info('Truncating domain')
            cursor.execute('TRUNCATE TABLE domain')

            logger.info('Restoring system_status setting to starting')
            cursor.execute(
                """
                UPDATE settings
                SET `value` = %s
                WHERE `id` = %s
                """,
                ('"starting"', 7),
            )

            logger.info('Re-enabling foreign key checks')
            cursor.execute('SET FOREIGN_KEY_CHECKS = 1')

        conn.commit()
        logger.info('Database reset completed')
    except Exception:
        conn.rollback()
        logger.exception('Database reset failed')
        raise
    finally:
        with suppress(Exception):
            with conn.cursor() as cursor:
                cursor.execute('SET FOREIGN_KEY_CHECKS = 1')
        close_connection()


if __name__ == '__main__':
    reset_db()
