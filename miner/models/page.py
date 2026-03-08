from dataclasses import dataclass
from datetime import datetime
import pymysql

from miner.db import get_connection
from miner.models.utils import md5_bin16, normalize_url
from miner.enums.page_status import PageStatus
from miner.settings.settings_db import SettingsDB

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
    def from_url(
        cls,
        url: str,
        recursion_level: int = 0,
        status: PageStatus = PageStatus.TODO,
    ) -> "Page":
        url = normalize_url(url)
        st = status.value if hasattr(status, "value") else str(status)
        return cls(
            id=None,
            domain_id=None,
            parent_page_id=None,
            same_as=None,
            url=url,
            url_md5=md5_bin16(url),
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
        self.url_final = normalize_url(url_final) if url_final else None
        self.url_final_md5 = md5_bin16(self.url_final) if self.url_final else None

    def set_text(self, text: str | None) -> None:
        self.text = text
        self.text_md5 = md5_bin16(text) if text else None

    def set_html(self, html: str | None) -> None:
        self.html = html
        self.html_md5 = md5_bin16(html) if html else None

    @classmethod
    def get_by_md5(cls, url_md5: bytes) -> "Page | None":
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
            return cls(**row) if row else None

    @classmethod
    def get_by_id(cls, page_id: int) -> "Page | None":
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
            return cls(**row) if row else None

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
    ) -> "Page":
        url = normalize_url(url)
        url_md5 = md5_bin16(url)

        existing = cls.get_by_md5(url_md5)
        if existing:
            return existing

        st = status.value if hasattr(status, "value") else str(status)

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
                    (domain_id, parent_page_id, same_as, url, url_md5, recursion_level, st),
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
    def claim_next_todo_url(cls) -> str | None:
        # prevent too many fast requests for the same domain
        domain_cooldown_ms = SettingsDB().get_config("domain_request_interval_ms")
        domain_cooldown_seconds = int(domain_cooldown_ms / 1000)

        # allow to retry failed pages after a while
        retry_interval_ms = SettingsDB().get_config("retry_interval_ms")
        retry_interval_seconds = int(retry_interval_ms / 1000)

        conn = get_connection()

        retryable_statuses = [
            PageStatus.FAILED.value,
        ]

        try:
            with conn.cursor() as cur:
                params: list[object] = []
                retry_clause = ""

                placeholders = ", ".join(["%s"] * len(retryable_statuses))
                retry_clause = f"""
                    OR (
                        p.status IN ({placeholders})
                        AND p.updated_at <= (
                            CURRENT_TIMESTAMP - INTERVAL %s SECOND
                        )
                    )
                """
                params.extend(retryable_statuses)
                params.append(retry_interval_seconds)

                cur.execute(
                    f"""
                    SELECT
                        p.id,
                        p.url
                    FROM pages p
                    INNER JOIN domain d ON d.id = p.domain_id
                    WHERE (
                            p.status = %s
                            {retry_clause}
                        )
                      AND (
                            d.last_request_at IS NULL
                            OR d.last_request_at <= (
                                CURRENT_TIMESTAMP - INTERVAL %s SECOND
                            )
                      )
                    ORDER BY
                        CASE
                            WHEN p.status = %s THEN 0
                            ELSE 1
                        END,
                        FLOOR(
                            TIMESTAMPDIFF(
                                SECOND,
                                p.created_at,
                                CURRENT_TIMESTAMP
                            ) / 30
                        ) ASC,
                        MOD(
                            CRC32(
                                CONCAT(
                                    p.id,
                                    '-',
                                    FLOOR(UNIX_TIMESTAMP(CURRENT_TIMESTAMP) / 10)
                                )
                            ),
                            1000000
                        ) ASC
                    LIMIT 1
                    FOR UPDATE SKIP LOCKED
                    """,
                    [
                        PageStatus.TODO.value,
                        *params,
                        domain_cooldown_seconds,
                        PageStatus.TODO.value,
                    ],
                )
                row = cur.fetchone()

                if not row:
                    conn.rollback()
                    return None

                cur.execute(
                    """
                    UPDATE pages
                    SET status = %s, updated_at = CURRENT_TIMESTAMP
                    WHERE id = %s
                    """,
                    (PageStatus.PROCESSING.value, row["id"]),
                )

            conn.commit()
            return row["url"]

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
            st = status.value if hasattr(status, "value") else str(status)
            sets.append("status = %s")
            params.append(st)
            self.status = st

        if domain_id is not None:
            sets.append("domain_id = %s")
            params.append(domain_id)
            self.domain_id = domain_id

        if parent_page_id is not None:
            sets.append("parent_page_id = %s")
            params.append(parent_page_id)
            self.parent_page_id = parent_page_id

        if recursion_level is not None:
            sets.append("recursion_level = %s")
            params.append(recursion_level)
            self.recursion_level = recursion_level

        if retry_count is not None:
            sets.append("retry_count = %s")
            params.append(retry_count)
            self.retry_count = retry_count

        if url_final is not None:
            normalized_url_final = normalize_url(url_final)
            new_url_final_md5 = md5_bin16(normalized_url_final)

            sets.append("url_final = %s")
            params.append(normalized_url_final)
            sets.append("url_final_md5 = %s")
            params.append(new_url_final_md5)

            self.url_final = normalized_url_final
            self.url_final_md5 = new_url_final_md5

        if status_code is not None:
            sets.append("status_code = %s")
            params.append(status_code)
            self.status_code = status_code

        if title is not None:
            sets.append("title = %s")
            params.append(title)
            self.title = title

        if text is not None:
            new_text_md5 = md5_bin16(text)
            sets.append("text = %s")
            params.append(text)
            sets.append("text_md5 = %s")
            params.append(new_text_md5)

        if html is not None:
            new_html_md5 = md5_bin16(html)
            sets.append("html = %s")
            params.append(html)
            sets.append("html_md5 = %s")
            params.append(new_html_md5)

        if same_as is not None:
            sets.append("same_as = %s")
            params.append(same_as)
            self.same_as = same_as

        if not sets:
            return 0

        sets.append("updated_at = CURRENT_TIMESTAMP")

        sql = f"UPDATE pages SET {', '.join(sets)} WHERE url_md5 = %s"
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

            if html is not None:
                self.html = html
                self.html_md5 = new_html_md5

            return int(affected)

        except pymysql.err.IntegrityError as e:
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
                            updated_at = CURRENT_TIMESTAMP
                        WHERE url_md5 = %s
                        """,
                        (duplicate_page_id, self.url_md5),
                    )
                    affected = cur.rowcount

                conn.commit()
                self.same_as = duplicate_page_id
                return int(affected)

            except Exception:
                conn.rollback()
                raise

        except Exception:
            conn.rollback()
            raise

    @classmethod
    def release_stucked_processing(cls, older_than_seconds: int = 60) -> int:
        """
        Move pages com status PROCESSING e updated_at mais antigo que X segundos de volta para TODO.
        Retorna a quantidade de registros afetados.
        """
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
                clauses.append("text_md5 = %s")
                params.append(text_md5)

            if html_md5 is not None:
                clauses.append("html_md5 = %s")
                params.append(html_md5)

            sql = f"""
                SELECT id
                FROM pages
                WHERE ({' OR '.join(clauses)})
            """

            if exclude_id is not None:
                sql += " AND id <> %s"
                params.append(exclude_id)

            sql += " ORDER BY id ASC LIMIT 1"

            cur.execute(sql, params)
            row = cur.fetchone()
            return int(row["id"]) if row else None