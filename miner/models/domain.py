from dataclasses import dataclass
from datetime import datetime
from miner.db import get_connection
from miner.models.utils import md5_bin16, extract_hostname
import pymysql
from miner.settings.settings_db import SettingsDB


@dataclass
class Domain:
    id: int | None
    url: str
    url_md5: bytes
    parent_domain_id: int | None
    recursion_level: int = 0

    request_count: int = 0
    last_request_at: datetime | None = None

    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def get_by_md5(cls, url_md5: bytes) -> 'Domain | None':
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    url,
                    url_md5,
                    parent_domain_id,
                    recursion_level,
                    request_count,
                    last_request_at,
                    created_at,
                    updated_at
                FROM domain
                WHERE url_md5=%s
                LIMIT 1
                """,
                (url_md5,),
            )
            row = cur.fetchone()
            return cls(**row) if row else None

    @classmethod
    def get_or_create(cls, any_url: str, parent_pager=None) -> 'Domain':

        dom = extract_hostname(any_url)
        dom_md5 = md5_bin16(dom)

        existing = cls.get_by_md5(dom_md5)
        if existing:
            return existing

        recursion_level = parent_pager.domain.recursion_level + 1 if parent_pager is not None else 0
        parent_id = parent_pager.domain.id if parent_pager is not None else None

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO domain (url, url_md5, recursion_level, parent_domain_id)
                    VALUES (%s, %s, %s, %s)
                    """,
                    (dom, dom_md5, recursion_level, parent_id),
                )
                new_id = cur.lastrowid

            conn.commit()

            return cls.get_by_id(int(new_id))

        except pymysql.err.IntegrityError:
            conn.rollback()
            existing = cls.get_by_md5(dom_md5)
            if existing:
                return existing
            raise

        except Exception:
            conn.rollback()
            raise

    @classmethod
    def get_by_id(cls, domain_id: int) -> 'Domain | None':
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    url,
                    url_md5,
                    parent_domain_id,
                    recursion_level,
                    request_count,
                    last_request_at,
                    created_at,
                    updated_at
                FROM domain
                WHERE id=%s
                LIMIT 1
                """,
                (domain_id,),
            )
            row = cur.fetchone()
            return cls(**row) if row else None

    def try_register_request(self) -> bool:
        domain_cooldown_ms = SettingsDB().get_config('domain_request_interval_ms')
        domain_cooldown_seconds = int(domain_cooldown_ms / 1000)
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE domain
                    SET request_count = request_count + 1,
                        last_request_at = NOW(),
                        updated_at = NOW()
                    WHERE id = %s
                      AND (
                            last_request_at IS NULL
                            OR last_request_at <= (NOW() - INTERVAL %s SECOND)
                      )
                    """,
                    (self.id, int(domain_cooldown_seconds)),
                )
                affected = cur.rowcount

            conn.commit()

            if affected:
                fresh = self.get_by_id(self.id)
                if fresh:
                    self.request_count = fresh.request_count
                    self.last_request_at = fresh.last_request_at
                    self.updated_at = fresh.updated_at
                return True

            return False

        except Exception:
            conn.rollback()
            raise

    @classmethod
    def extract_hostname(cls, url):
        return extract_hostname(url)

    @classmethod
    def bulk_get_or_create(cls, urls: list[str], parent_domain) -> dict[str, 'Domain']:
        conn = get_connection()

        extracted_hosts = [extract_hostname(url) for url in urls]
        hosts = list({host for host in extracted_hosts if host})

        if not hosts:
            return {}

        rows = []
        for host in hosts:
            rows.append(
                (
                    host,
                    md5_bin16(host),
                    parent_domain.recursion_level + 1 if parent_domain is not None else 0,
                    parent_domain.id if parent_domain is not None else None,
                )
            )

        md5s = list(map(lambda x: x[1], rows))

        with conn.cursor() as cur:
            cur.executemany(
                """
                INSERT IGNORE INTO domain (url, url_md5, recursion_level, parent_domain_id)
                VALUES (%s, %s, %s, %s)
                """,
                rows,
            )

            format_strings = ','.join(['%s'] * len(hosts))
            cur.execute(
                f"""
                SELECT
                    id, url, url_md5, parent_domain_id, recursion_level,
                    request_count, last_request_at, created_at, updated_at
                FROM domain
                WHERE url_md5 IN ({format_strings})
                """,
                md5s,
            )
            result = cur.fetchall()

        conn.commit()
        return {row['url']: cls(**row) for row in result}
