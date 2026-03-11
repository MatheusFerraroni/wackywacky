import logging
import pymysql
from miner.settings.settings import settings

logger = logging.getLogger('leader')


class LeaderElection:
    def __init__(self, lock_name: str = 'miner:global-leader'):
        self.lock_name = lock_name
        self.conn = None
        self.is_leader = False

    def _connect(self):
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
        if self.conn is None:
            self.conn = self._connect()

        with self.conn.cursor() as cur:
            cur.execute('SELECT GET_LOCK(%s, %s) AS acquired', (self.lock_name, timeout_seconds))
            row = cur.fetchone()

        self.is_leader = bool(row and row['acquired'] == 1)
        return self.is_leader

    def refresh(self) -> bool:
        if self.conn is None:
            self.is_leader = False
            return False

        try:
            self.conn.ping(reconnect=False)
            return self.is_leader
        except Exception:
            self.is_leader = False
            return False

    def release(self):
        if self.conn is None:
            return

        try:
            with self.conn.cursor() as cur:
                cur.execute('SELECT RELEASE_LOCK(%s) AS released', (self.lock_name,))
        except Exception:
            pass
        finally:
            try:
                self.conn.close()
            except Exception:
                pass
            self.conn = None
            self.is_leader = False
