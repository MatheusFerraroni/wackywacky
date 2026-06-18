"""Page model and persistence helpers."""

import threading
from dataclasses import dataclass
from datetime import datetime

import pymysql
import zstandard as zstd

from miner.db import get_connection
from miner.enums.page_status import PageStatus
from miner.metrics import metric_pages_marked_as_same_as
from miner.models.utils import md5_bin16, normalize_url
from miner.settings.settings import settings
from miner.settings.settings_db import SettingsDB

lock_claim_next = threading.RLock()
CLAIM_BATCH_SIZE = 20

_ZSTD_LEVEL = 11
_MYSQL_MEDIUMBLOB_MAX_BYTES = 16_777_215
_TITLE_MAX_CHARACTERS = 512
_zstd_compressor = zstd.ZstdCompressor(level=_ZSTD_LEVEL)
_zstd_decompressor = zstd.ZstdDecompressor()

_claim_next_ids: list[int] = []


def _compress_str(value: str | None) -> bytes | None:
    """Compress a string for storage."""
    if value is None:
        return None
    return _zstd_compressor.compress(value.encode('utf-8'))


def _prepare_content_for_storage(value: str) -> tuple[str, bytes]:
    """Return content and compressed bytes that fit the configured DB column."""
    value = value[: settings.MAX_CHARACTERS_TEXT]
    compressed = _compress_str(value)
    if compressed is None:
        return value, b''

    if len(compressed) <= _MYSQL_MEDIUMBLOB_MAX_BYTES:
        return value, compressed

    low = 0
    high = len(value)
    best_value = ''
    best_compressed = b''

    while low <= high:
        mid = (low + high) // 2
        candidate_value = value[:mid]
        candidate_compressed = _compress_str(candidate_value) or b''

        if len(candidate_compressed) <= _MYSQL_MEDIUMBLOB_MAX_BYTES:
            best_value = candidate_value
            best_compressed = candidate_compressed
            low = mid + 1
        else:
            high = mid - 1

    return best_value, best_compressed


def _decompress_str(value: bytes | None) -> str | None:
    """Decompress a stored string."""
    if value is None:
        return None
    return _zstd_decompressor.decompress(value).decode('utf-8')


@dataclass
class Page:  # pylint: disable=too-many-instance-attributes
    """Persisted crawl page."""

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
    def url_to_md5(cls, url: str) -> bytes:
        """Return the 16-byte MD5 digest for a normalized URL."""
        if not isinstance(url, str):
            raise TypeError('URL must be a string')
        return md5_bin16(cls.normalize_url(url))

    @classmethod
    def normalize_url(cls, url):
        """Normalize a URL for storage."""
        return normalize_url(url)

    @classmethod
    def from_db_row(cls, row: dict) -> 'Page':
        """Build a Page from a database row."""
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
            text=row['text'],
            text_md5=row['text_md5'],
            html=row['html'],
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
        """Build a new unsaved Page from a URL."""
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
        """Set the final normalized URL and its digest."""
        self.url_final = self.normalize_url(url_final) if url_final else None
        self.url_final_md5 = self.url_to_md5(self.url_final) if self.url_final else None

    def set_text(self, text: str | None) -> None:
        """Set page text respecting the configured size limit."""
        if text is None:
            return
        self.text, _ = _prepare_content_for_storage(text)
        self.text_md5 = md5_bin16(self.text) if self.text else None

    def set_html(self, html: str | None) -> None:
        """Set page HTML when HTML persistence is enabled."""
        if not settings.SAVE_HTML:
            return
        self.html, _ = _prepare_content_for_storage(html)
        self.html_md5 = md5_bin16(self.html) if self.html else None

    @classmethod
    def get_by_md5(cls, url_md5: bytes) -> 'Page | None':
        """Load a page by URL MD5 digest."""
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id, domain_id, parent_page_id, same_as,
                    url, url_md5,
                    url_final, url_final_md5,
                    status_code, title,
                    recursion_level, status,
                    retry_count,
                    text, text_md5, html, html_md5,
                    created_at, updated_at
                FROM pages
                WHERE url_md5 = %s
                LIMIT 1
                """,
                (url_md5,),
            )
            row = cur.fetchone()
            return cls.from_db_row(row) if row else None

    @classmethod
    def get_by_id(cls, page_id: int) -> 'Page | None':
        """Load a page by primary key."""
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id, domain_id, parent_page_id, same_as,
                    url, url_md5,
                    url_final, url_final_md5,
                    status_code, title,
                    recursion_level, status,
                    retry_count,
                    text, text_md5, html, html_md5,
                    created_at, updated_at
                FROM pages
                WHERE id = %s
                LIMIT 1
                """,
                (page_id,),
            )
            row = cur.fetchone()
            return cls.from_db_row(row) if row else None

    @classmethod
    # pylint: disable-next=too-many-arguments
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
        """Return an existing page or create it."""
        url = cls.normalize_url(url)
        url_md5 = cls.url_to_md5(url)

        existing = cls.get_by_md5(url_md5)
        if existing:
            return existing

        st = status.value if hasattr(status, 'value') else str(status)

        conn = get_connection()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    INSERT INTO pages (
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
                new_id = cur.lastrowid

            conn.commit()
            return cls.get_by_id(int(new_id))

        except pymysql.err.IntegrityError:
            conn.rollback()
            existing = cls.get_by_md5(url_md5)
            if existing:
                return existing
            raise
        except Exception:
            conn.rollback()
            raise

    @classmethod
    # pylint: disable-next=too-many-locals
    def claim_next_todo_url(
        cls,
    ) -> int | None:
        """Claim the next eligible page id for this process."""
        # pylint: disable=global-statement
        global _claim_next_ids
        conn = None

        try:
            with lock_claim_next:
                if _claim_next_ids:
                    return _claim_next_ids.pop(0)

                settings_db = SettingsDB()
                domain_cooldown_ms = int(settings_db.get_config('domain_request_interval_ms'))
                retry_interval_ms = int(settings_db.get_config('retry_interval_ms'))

                domain_cooldown_us = domain_cooldown_ms * 1000
                retry_interval_us = retry_interval_ms * 1000

                max_recursion = settings_db.get_config('max_recursion')
                max_recursion_page = settings_db.get_config('max_recursion_page')
                max_retry_attempts = settings_db.get_config('max_retry_attempts')

                conn = get_connection()

                def select_batch(cur, status: PageStatus) -> list[dict]:
                    retry_clause = ''
                    prior_retry_clause = ''
                    params = [
                        max_recursion_page,
                        max_recursion,
                        max_retry_attempts,
                        domain_cooldown_us,
                        status.value,
                    ]

                    if status == PageStatus.FAILED:
                        retry_clause = (
                            'AND p.updated_at <= CURRENT_TIMESTAMP(6) - INTERVAL %s MICROSECOND'
                        )
                        prior_retry_clause = (
                            'AND prior.updated_at <= '
                            'CURRENT_TIMESTAMP(6) - INTERVAL %s MICROSECOND'
                        )
                        params.append(retry_interval_us)

                    params.extend(
                        [
                            max_recursion_page,
                            max_retry_attempts,
                            status.value,
                        ]
                    )

                    if status == PageStatus.FAILED:
                        params.append(retry_interval_us)

                    params.append(CLAIM_BATCH_SIZE)

                    cur.execute(
                        f"""
                        SELECT
                            p.id,
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
                                OR d.last_request_at <= (
                                    CURRENT_TIMESTAMP(6) - INTERVAL %s MICROSECOND
                                )
                            )
                            AND p.status = %s
                            {retry_clause}
                            AND NOT EXISTS (
                                SELECT 1
                                FROM pages prior
                                WHERE prior.domain_id = p.domain_id
                                  AND prior.recursion_level < %s
                                  AND prior.retry_count < %s
                                  AND prior.status = %s
                                  {prior_retry_clause}
                                  AND prior.id < p.id
                            )
                        LIMIT %s
                        FOR UPDATE SKIP LOCKED
                        """,
                        params,
                    )
                    return cur.fetchall()

                with conn.cursor() as cur:
                    rows = select_batch(cur, PageStatus.TODO)

                    if not rows:
                        rows = select_batch(cur, PageStatus.FAILED)

                    if not rows:
                        conn.rollback()
                        return None

                    claimed_ids = [int(row['id']) for row in rows]
                    domain_ids = sorted({int(row['domain_id']) for row in rows})

                    pages_in_clause = ', '.join(['%s'] * len(claimed_ids))
                    cur.execute(
                        f"""
                        UPDATE pages
                        SET status = %s, updated_at = CURRENT_TIMESTAMP
                        WHERE id IN ({pages_in_clause})
                        """,
                        [PageStatus.PROCESSING.value, *claimed_ids],
                    )

                    if cur.rowcount != len(claimed_ids):
                        raise RuntimeError(
                            f'Claim batch update mismatch: expected={len(claimed_ids)} '
                            f'affected={cur.rowcount}'
                        )

                    domains_in_clause = ', '.join(['%s'] * len(domain_ids))
                    cur.execute(
                        f"""
                        UPDATE domain
                        SET updated_at = CURRENT_TIMESTAMP(6)
                        WHERE id IN ({domains_in_clause})
                        """,
                        domain_ids,
                    )

                conn.commit()
                _claim_next_ids = claimed_ids
                return _claim_next_ids.pop(0)
        except Exception:
            if conn is not None:
                conn.rollback()
            _claim_next_ids = []
            raise

    @classmethod
    def get_id_by_text_or_html_md5(
        cls,
        *,
        text_md5: bytes | None = None,
        html_md5: bytes | None = None,
        exclude_id: int | None = None,
    ) -> int | None:
        """Find another page with the same text or HTML digest."""
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

    # pylint: disable-next=too-many-arguments,too-many-positional-arguments,too-many-locals,too-many-branches,too-many-statements
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
        """Update this page in the database and keep local fields in sync."""
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
            title = title[: min(settings.MAX_CHARACTERS_TEXT, _TITLE_MAX_CHARACTERS)]
            sets.append('title = %s')
            params.append(title)
            self.title = title

        if text is not None:
            text, compressed_text = _prepare_content_for_storage(text)
            new_text_md5 = md5_bin16(text)
            sets.append('text = %s')
            params.append(compressed_text)
            sets.append('text_md5 = %s')
            params.append(new_text_md5)

        if html is not None and settings.SAVE_HTML:
            html, compressed_html = _prepare_content_for_storage(html)
            new_html_md5 = md5_bin16(html)
            sets.append('html = %s')
            params.append(compressed_html)
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
                self.text = text
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
    def release_stucked_processing(cls, older_than_seconds: int | None = None) -> int:
        """
        Move pages com status PROCESSING e updated_at mais antigo que X segundos de volta para TODO.
        Retorna a quantidade de registros afetados.
        """
        if older_than_seconds is None:
            older_than_seconds = settings.PROCESSING_TIMEOUT_SECONDS

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
    # pylint: disable-next=too-many-locals
    def bulk_insert_ignore(cls, rows: list[dict]) -> int:
        """Bulk insert pages while ignoring duplicates."""
        if not rows:
            return 0

        conn = get_connection()
        batch_size = 1000
        total_affected = 0

        seen: set[bytes] = set()
        prepared_rows: list[tuple] = []

        for row in rows:
            raw_url = row['url']
            normalized_url = cls.normalize_url(raw_url)
            url_md5 = cls.url_to_md5(normalized_url)

            if url_md5 in seen:
                continue
            seen.add(url_md5)

            prepared_rows.append(
                (
                    row['domain_id'],
                    row['parent_page_id'],
                    row['same_as'],
                    normalized_url,
                    url_md5,
                    row['recursion_level'],
                    row['status'],
                )
            )

        if not prepared_rows:
            return 0

        base_sql = """
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

        with conn.cursor() as cur:
            for i in range(0, len(prepared_rows), batch_size):
                batch = prepared_rows[i : i + batch_size]

                values_sql = ', '.join(['(%s, %s, %s, %s, %s, %s, %s)'] * len(batch))
                sql = base_sql.format(values_sql=values_sql)

                params = []
                for item in batch:
                    params.extend(item)

                retry_counter = 0

                while True:
                    try:
                        cur.execute(sql, params)
                        break
                    except pymysql.MySQLError as e:
                        retry_counter += 1
                        if retry_counter >= 3:
                            raise e
                total_affected += cur.rowcount

        conn.commit()
        return total_affected
