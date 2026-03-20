from dataclasses import dataclass
from datetime import datetime
import random
import pymysql
import re
import threading

import zstandard as zstd

from miner.db import get_connection
from miner.models.utils import md5_bin16, normalize_url
from miner.enums.page_status import PageStatus
from miner.settings.settings_db import SettingsDB
from miner.settings.settings import settings
from miner.metrics import metric_pages_marked_as_same_as

lock_claim_next = threading.RLock()

_ZSTD_LEVEL = 11
_zstd_compressor = zstd.ZstdCompressor(level=_ZSTD_LEVEL)
_zstd_decompressor = zstd.ZstdDecompressor()

MD5_REGEX = re.compile(r'^[a-fA-F0-9]{32}$')


def _compress_str(value: str | None) -> bytes | None:
    if value is None:
        return None
    return _zstd_compressor.compress(value.encode('utf-8'))


def _decompress_str(value: bytes | None) -> str | None:
    if value is None:
        return None
    return _zstd_decompressor.decompress(value).decode('utf-8')


@dataclass
class Page:
    id: int | None
    domain_id: int | None
    parent_page_id: int | None
    same_as: int | None

    url: str
    url_md5: bytes

    url_final: str | None = None
    url_final_md5: bytes | None = None

    status_code: int | None = None
    title: str | None = None

    recursion_level: int = 0
    status: str = PageStatus.TODO

    retry_count: int = 0

    text: str | None = None
    text_md5: bytes | None = None
    html: str | None = None
    html_md5: bytes | None = None

    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def url_to_md5(cls, url):
        if type(url) is not str or bool(MD5_REGEX.fullmatch(url)):
            return url
        return md5_bin16(cls.normalize_url(url))

    @classmethod
    def normalize_url(cls, url):
        return normalize_url(url)

    @classmethod
    def from_db_row(cls, row: dict) -> 'Page':
        return cls(
            id=row['id'],
            domain_id=row['domain_id'],
            parent_page_id=row['parent_page_id'],
            same_as=row['same_as'],
            url=row['url'],
            url_md5=row['url_md5'],
            url_final=row['url_final'],
            url_final_md5=row['url_final_md5'],
            status_code=row['status_code'],
            title=row['title'],
            recursion_level=row['recursion_level'],
            status=row['status'],
            retry_count=row['retry_count'],
            text=_decompress_str(row['text']) if row['text'] is not None else None,
            text_md5=row['text_md5'],
            html=_decompress_str(row['html']) if row['html'] is not None else None,
            html_md5=row['html_md5'],
            created_at=row['created_at'],
            updated_at=row['updated_at'],
        )

    @classmethod
    def from_url(
        cls,
        url: str,
        recursion_level: int = 0,
        status: PageStatus = PageStatus.TODO,
    ) -> 'Page':
        url = cls.normalize_url(url)
        st = status.value if hasattr(status, 'value') else str(status)
        return cls(
            id=None,
            domain_id=None,
            parent_page_id=None,
            same_as=None,
            url=url,
            url_md5=cls.url_to_md5(url),
            url_final=None,
            url_final_md5=None,
            status_code=None,
            title=None,
            recursion_level=recursion_level,
            status=st,
            retry_count=0,
            text=None,
            text_md5=None,
            html=None,
            html_md5=None,
            created_at=None,
            updated_at=None,
        )

    def set_url_final(self, url_final: str | None) -> None:
        self.url_final = self.normalize_url(url_final) if url_final else None
        self.url_final_md5 = self.url_to_md5(self.url_final) if self.url_final else None

    def set_text(self, text: str | None) -> None:
        if text is None:
            return
        self.text = text[: settings.MAX_CHARACTERS_TEXT]
        self.text_md5 = self.url_to_md5(self.text) if self.text else None

    def set_html(self, html: str | None) -> None:
        if not settings.SAVE_HTML:
            return
        self.html = html
        self.html_md5 = self.url_to_md5(html) if html else None

    @classmethod
    def get_by_md5(cls, url_md5: bytes) -> 'Page | None':
        conn = get_connection()
        query = """
            SELECT
                id, domain_id, parent_page_id, same_as,
                url, url_md5,
                url_final, url_final_md5,
                status_code, title,
                recursion_level, status,
                retry_count,
                text, text_md5, html, html_md5,
                created_at, updated_at
            FROM {table}
            WHERE url_md5 = %s
            LIMIT 1
        """

        with conn.cursor() as cur:
            for table in ('pages', 'pages_complete'):
                cur.execute(query.format(table=table), (url_md5,))
                row = cur.fetchone()
                if row:
                    return cls.from_db_row(row)

        return None

    @classmethod
    def get_by_id(cls, page_id: int) -> 'Page | None':
        conn = get_connection()
        query = """
            SELECT
                id, domain_id, parent_page_id, same_as,
                url, url_md5,
                url_final, url_final_md5,
                status_code, title,
                recursion_level, status,
                retry_count,
                text, text_md5, html, html_md5,
                created_at, updated_at
            FROM {table}
            WHERE id = %s
            LIMIT 1
        """

        with conn.cursor() as cur:
            for table in ('pages', 'pages_complete'):
                cur.execute(query.format(table=table), (page_id,))
                row = cur.fetchone()
                if row:
                    return cls.from_db_row(row)

        return None

    @classmethod
    def get_or_create(
        cls,
        *,
        domain_id: int,
        url: str,
        parent_page_id: int | None = None,
        same_as: int | None = None,
        recursion_level: int = 0,
        status: PageStatus = PageStatus.TODO,
    ) -> 'Page':
        url = cls.normalize_url(url)
        url_md5 = cls.url_to_md5(url)
        st = status.value if hasattr(status, 'value') else str(status)

        existing = cls.get_by_md5(url_md5)
        if existing:
            return existing

        conn = get_connection()

        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT IGNORE INTO pages (
                        domain_id,
                        parent_page_id,
                        same_as,
                        url,
                        url_md5,
                        recursion_level,
                        status
                    )
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    """,
                    (
                        domain_id,
                        parent_page_id,
                        same_as,
                        url,
                        url_md5,
                        recursion_level,
                        st,
                    ),
                )

                new_id = cur.lastrowid if cur.rowcount > 0 else None

                cur.execute(
                    """
                    INSERT IGNORE INTO cache_url (url_md5, url)
                    VALUES (%s, %s)
                    """,
                    (url_md5, url),
                )

            conn.commit()

            if new_id:
                page = cls.get_by_id(int(new_id))
                if page:
                    return page

            existing = cls.get_by_md5(url_md5)
            if existing:
                return existing

            raise RuntimeError('Falha ao criar ou localizar página após INSERT IGNORE.')

        except Exception:
            conn.rollback()
            raise

    @classmethod
    def claim_next_todo_url(cls) -> int | None:
        domain_cooldown_ms = SettingsDB().get_config('domain_request_interval_ms')
        domain_cooldown_seconds = int(domain_cooldown_ms / 1000)

        retry_interval_ms = SettingsDB().get_config('retry_interval_ms')
        retry_interval_seconds = int(retry_interval_ms / 1000)

        conn = get_connection()

        max_recursion = SettingsDB().get_config('max_recursion')
        max_recursion_page = SettingsDB().get_config('max_recursion_page')
        max_retry_attempts = SettingsDB().get_config('max_retry_attempts')

        try:
            with conn.cursor() as cur:
                with lock_claim_next:
                    cur.execute(
                        """
                        SELECT
                            p.id,
                            p.url,
                            d.id AS domain_id
                        FROM pages p
                        INNER JOIN domain d
                            ON d.id = p.domain_id
                        WHERE
                            p.recursion_level < %s
                            AND d.recursion_level < %s
                            AND p.retry_count < %s
                            AND (
                                d.last_request_at IS NULL
                                OR d.last_request_at <= CURRENT_TIMESTAMP - INTERVAL %s SECOND
                            )
                            AND p.status = %s
                        LIMIT 20
                        FOR UPDATE SKIP LOCKED
                        """,
                        [
                            max_recursion_page,
                            max_recursion,
                            max_retry_attempts,
                            domain_cooldown_seconds,
                            PageStatus.TODO.value,
                        ],
                    )

                    rows = cur.fetchall()

                    if not rows:
                        cur.execute(
                            """
                            SELECT
                                p.id,
                                p.url,
                                d.id AS domain_id
                            FROM pages p
                            INNER JOIN domain d
                                ON d.id = p.domain_id
                            WHERE
                                p.recursion_level < %s
                                AND d.recursion_level < %s
                                AND p.retry_count < %s
                                AND (
                                    d.last_request_at IS NULL
                                    OR d.last_request_at <= CURRENT_TIMESTAMP - INTERVAL %s SECOND
                                )
                                AND p.status = %s
                                AND p.updated_at <= CURRENT_TIMESTAMP - INTERVAL %s SECOND
                            LIMIT 20
                            FOR UPDATE SKIP LOCKED
                            """,
                            [
                                max_recursion_page,
                                max_recursion,
                                max_retry_attempts,
                                domain_cooldown_seconds,
                                PageStatus.FAILED.value,
                                retry_interval_seconds,
                            ],
                        )

                        rows = cur.fetchall()

                    if not rows:
                        conn.rollback()
                        return None

                    row = random.choice(rows)

                    cur.execute(
                        """
                        UPDATE pages
                        SET status = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        """,
                        (PageStatus.PROCESSING.value, row['id']),
                    )

                    cur.execute(
                        """
                        UPDATE domain
                        SET updated_at = CURRENT_TIMESTAMP
                        WHERE id = %s
                        """,
                        (row['domain_id'],),
                    )

                conn.commit()
                return int(row['id'])

        except Exception:
            conn.rollback()
            raise

    def update(
        self,
        status: PageStatus | str | None = None,
        domain_id: int | None = None,
        parent_page_id: int | None = None,
        same_as: int | None = None,
        recursion_level: int | None = None,
        retry_count: int | None = None,
        url_final: str | None = None,
        status_code: int | None = None,
        title: str | None = None,
        text: str | None = None,
        html: str | None = None,
    ) -> int:
        sets: list[str] = []
        params: list[object] = []

        new_text_md5: bytes | None = None
        new_html_md5: bytes | None = None

        if status is not None:
            st = status.value if hasattr(status, 'value') else str(status)
            sets.append('status = %s')
            params.append(st)
            self.status = st

        if domain_id is not None:
            sets.append('domain_id = %s')
            params.append(domain_id)
            self.domain_id = domain_id

        if parent_page_id is not None:
            sets.append('parent_page_id = %s')
            params.append(parent_page_id)
            self.parent_page_id = parent_page_id

        if recursion_level is not None:
            sets.append('recursion_level = %s')
            params.append(recursion_level)
            self.recursion_level = recursion_level

        if retry_count is not None:
            sets.append('retry_count = %s')
            params.append(retry_count)
            self.retry_count = retry_count

        if url_final is not None:
            normalized_url_final = self.normalize_url(url_final)
            new_url_final_md5 = self.url_to_md5(normalized_url_final)

            sets.append('url_final = %s')
            params.append(normalized_url_final)
            sets.append('url_final_md5 = %s')
            params.append(new_url_final_md5)

            self.url_final = normalized_url_final
            self.url_final_md5 = new_url_final_md5

        if status_code is not None:
            sets.append('status_code = %s')
            params.append(status_code)
            self.status_code = status_code

        if title is not None:
            sets.append('title = %s')
            params.append(title)
            self.title = title

        if text is not None:
            text = text[: settings.MAX_CHARACTERS_TEXT]
            new_text_md5 = self.url_to_md5(text)
            sets.append('text = %s')
            params.append(_compress_str(text))
            sets.append('text_md5 = %s')
            params.append(new_text_md5)

        if html is not None and settings.SAVE_HTML:
            new_html_md5 = self.url_to_md5(html)
            sets.append('html = %s')
            params.append(_compress_str(html))
            sets.append('html_md5 = %s')
            params.append(new_html_md5)

        if same_as is not None:
            sets.append('same_as = %s')
            params.append(same_as)
            self.same_as = same_as

        if not sets:
            return 0

        sets.append('updated_at = CURRENT_TIMESTAMP')

        sql = f'UPDATE pages SET {", ".join(sets)} WHERE url_md5 = %s'
        args = [*params, self.url_md5]

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(sql, args)
                affected = cur.rowcount
            conn.commit()

            if text is not None:
                self.text = text[: settings.MAX_CHARACTERS_TEXT]
                self.text_md5 = new_text_md5

            if html is not None and settings.SAVE_HTML:
                self.html = html
                self.html_md5 = new_html_md5

            return int(affected)

        except pymysql.err.IntegrityError:
            conn.rollback()

            duplicate_page_id = self.get_id_by_text_or_html_md5(
                text_md5=new_text_md5,
                html_md5=new_html_md5,
                exclude_id=self.id,
            )

            if duplicate_page_id is None:
                raise

            try:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE pages
                        SET same_as = %s,
                            status = %s,
                            updated_at = CURRENT_TIMESTAMP
                        WHERE url_md5 = %s
                        """,
                        (duplicate_page_id, PageStatus.DONE.value, self.url_md5),
                    )
                    affected = cur.rowcount

                conn.commit()
                self.same_as = duplicate_page_id
                metric_pages_marked_as_same_as.add(1, {'service': 'miner'})
                return int(affected)

            except Exception:
                conn.rollback()
                raise

        except Exception:
            conn.rollback()
            raise

    @classmethod
    def release_stucked_processing(cls, older_than_seconds: int = 60) -> int:
        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE pages
                    SET status = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE status = %s
                      AND updated_at < (CURRENT_TIMESTAMP - INTERVAL %s SECOND)
                    """,
                    (
                        PageStatus.TODO.value,
                        PageStatus.PROCESSING.value,
                        int(older_than_seconds),
                    ),
                )
                affected = cur.rowcount

            conn.commit()
            return int(affected)

        except Exception:
            conn.rollback()
            raise

    @classmethod
    def get_id_by_text_or_html_md5(
        cls,
        *,
        text_md5: bytes | None = None,
        html_md5: bytes | None = None,
        exclude_id: int | None = None,
    ) -> int | None:
        if not text_md5 and not html_md5:
            return None

        conn = get_connection()
        with conn.cursor() as cur:
            clauses: list[str] = []
            params: list[object] = []

            if text_md5 is not None:
                clauses.append('text_md5 = %s')
                params.append(text_md5)

            if html_md5 is not None and settings.SAVE_HTML:
                clauses.append('html_md5 = %s')
                params.append(html_md5)

            sql = f"""
                SELECT id
                FROM pages
                WHERE ({' OR '.join(clauses)})
            """

            if exclude_id is not None:
                sql += ' AND id <> %s'
                params.append(exclude_id)

            sql += ' ORDER BY id ASC LIMIT 1'

            cur.execute(sql, params)
            row = cur.fetchone()
            return int(row['id']) if row else None

    @classmethod
    def bulk_insert_ignore(cls, rows: list[dict]) -> int:
        if not rows:
            return 0

        conn = get_connection()
        batch_size = 1000
        total_affected = 0
        seen: set[bytes] = set()

        prepared_rows: list[tuple] = []
        cache_rows: list[tuple[bytes, str]] = []

        for row in rows:
            raw_url = row['url']
            normalized_url = cls.normalize_url(raw_url)
            url_md5 = cls.url_to_md5(normalized_url)

            if url_md5 in seen:
                continue
            seen.add(url_md5)

            status = row['status']
            st = status.value if hasattr(status, 'value') else str(status)

            prepared_rows.append(
                (
                    row['domain_id'],
                    row['parent_page_id'],
                    row['same_as'],
                    normalized_url,
                    url_md5,
                    row['recursion_level'],
                    st,
                )
            )

            cache_rows.append((url_md5, normalized_url))

        if not prepared_rows:
            return 0

        pages_sql_base = """
            INSERT IGNORE INTO pages (
                domain_id,
                parent_page_id,
                same_as,
                url,
                url_md5,
                recursion_level,
                status
            )
            VALUES {values_sql}
        """

        cache_sql_base = """
            INSERT IGNORE INTO cache_url (
                url_md5,
                url
            )
            VALUES {values_sql}
        """

        existing_sql_base = """
            SELECT url_md5
            FROM pages
            WHERE url_md5 IN ({placeholders})
            UNION
            SELECT url_md5
            FROM pages_complete
            WHERE url_md5 IN ({placeholders})
        """

        try:
            with conn.cursor() as cur:
                for i in range(0, len(prepared_rows), batch_size):
                    batch_pages = prepared_rows[i : i + batch_size]
                    batch_cache = cache_rows[i : i + batch_size]

                    md5_list = [item[4] for item in batch_pages]
                    placeholders = ', '.join(['%s'] * len(md5_list))
                    existing_sql = existing_sql_base.format(placeholders=placeholders)

                    cur.execute(existing_sql, [*md5_list, *md5_list])
                    existing_rows = cur.fetchall()
                    existing_md5s = {
                        row['url_md5'] if isinstance(row, dict) else row[0] for row in existing_rows
                    }

                    pages_to_insert = []
                    cache_to_insert = []

                    for page_item, cache_item in zip(batch_pages, batch_cache):
                        page_url_md5 = page_item[4]
                        if page_url_md5 in existing_md5s:
                            continue

                        pages_to_insert.append(page_item)
                        cache_to_insert.append(cache_item)

                    if pages_to_insert:
                        pages_values_sql = ', '.join(
                            ['(%s, %s, %s, %s, %s, %s, %s)'] * len(pages_to_insert)
                        )
                        pages_sql = pages_sql_base.format(values_sql=pages_values_sql)

                        pages_params = []
                        for item in pages_to_insert:
                            pages_params.extend(item)

                        cur.execute(pages_sql, pages_params)
                        total_affected += cur.rowcount

                    if batch_cache:
                        cache_values_sql = ', '.join(['(%s, %s)'] * len(batch_cache))
                        cache_sql = cache_sql_base.format(values_sql=cache_values_sql)

                        cache_params = []
                        for item in batch_cache:
                            cache_params.extend(item)

                        cur.execute(cache_sql, cache_params)

            conn.commit()
            return int(total_affected)

        except Exception:
            conn.rollback()
            raise
