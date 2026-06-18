"""MySQL advisory lock based leader election."""

import logging

import pymysql

from miner.settings.settings import settings

logger = logging.getLogger('leader')


class LeaderElection:
    """Acquire and refresh the global miner leader lock."""

    def __init__(self, lock_name: str = 'miner:global-leader'):
        """Create a leader election handle for the given lock name."""
        self.lock_name = lock_name
        self.conn = None
        self.is_leader = False

    def _connect(self):
        """Open a dedicated autocommit connection for advisory locks."""
        return pymysql.connect(
            host=settings.DB_HOST,
            port=settings.DB_PORT,
            user=settings.DB_USER,
            password=settings.DB_PASSWORD,
            database=settings.DB_NAME,
            cursorclass=pymysql.cursors.DictCursor,
            autocommit=True,
        )

    def acquire(self, timeout_seconds: int = 0) -> bool:
        """Try to acquire the configured advisory lock."""
        if self.conn is None:
            self.conn = self._connect()

        with self.conn.cursor() as cur:
            cur.execute('SELECT GET_LOCK(%s, %s) AS acquired', (self.lock_name, timeout_seconds))
            row = cur.fetchone()

        self.is_leader = bool(row and row['acquired'] == 1)
        return self.is_leader

    def refresh(self) -> bool:
        """Verify that the leader connection is still alive."""
        if self.conn is None:
            self.is_leader = False
            return False

        try:
            self.conn.ping(reconnect=False)
            return self.is_leader
        except pymysql.MySQLError:
            self.is_leader = False
            return False

    def release(self):
        """Release the advisory lock and close the leader connection."""
        if self.conn is None:
            return

        try:
            with self.conn.cursor() as cur:
                cur.execute('SELECT RELEASE_LOCK(%s) AS released', (self.lock_name,))
        except pymysql.MySQLError:
            pass
        finally:
            try:
                self.conn.close()
            except pymysql.MySQLError:
                pass
            self.conn = None
            self.is_leader = False
