from dataclasses import dataclass

from miner.db import get_connection
from miner.enums.page_status import PageStatus
from miner.models import Page


@dataclass
class PageComplete(Page):
    @classmethod
    def transfer_to_complete(cls) -> int:
        conn = get_connection()

        statuses = (
            PageStatus.DONE.value,
            PageStatus.FAILED_TIMEOUT.value,
            PageStatus.DOMAIN_BLOCKED.value,
            PageStatus.BLOCKED_LIMIT_RECURSION.value,
            PageStatus.BLOCKED_LANGUAGE.value,
        )

        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""
                    SELECT id, url, url_md5
                    FROM pages
                    WHERE status IN ({', '.join(['%s'] * len(statuses))})
                    LIMIT 1000
                    """,
                    statuses,
                )
                rows = cur.fetchall()

                if not rows:
                    conn.commit()
                    return 0

                ids = [row['id'] for row in rows]
                url_md5_list = [row['url_md5'] for row in rows]

                id_placeholders = ', '.join(['%s'] * len(ids))
                md5_placeholders = ', '.join(['%s'] * len(url_md5_list))

                cache_rows = []
                for row in rows:
                    url = row['url']
                    if url:
                        cache_rows.append((Page.url_to_md5(url), Page.normalize_url(url)))

                if cache_rows:
                    cur.executemany(
                        """
                        INSERT IGNORE INTO cache_url (url_md5, url)
                        VALUES (%s, %s)
                        """,
                        cache_rows,
                    )

                cur.execute(
                    f"""
                    INSERT IGNORE INTO pages_complete (
                        id, domain_id, parent_page_id, same_as,
                        url, url_md5, url_final, url_final_md5,
                        status_code, title, recursion_level, status,
                        retry_count, text, html, text_md5, html_md5,
                        created_at, updated_at
                    )
                    SELECT id, domain_id, parent_page_id, same_as,
                        url, url_md5, url_final, url_final_md5,
                        status_code, title, recursion_level, status,
                        retry_count, text, html, text_md5, html_md5,
                        created_at, updated_at
                    FROM pages
                    WHERE id IN ({id_placeholders})
                    """,
                    ids,
                )

                cur.execute(
                    f"""
                    SELECT id
                    FROM pages
                    WHERE id IN ({id_placeholders})
                      AND url_md5 IN (
                          SELECT url_md5
                          FROM pages_complete
                          WHERE url_md5 IN ({md5_placeholders})
                      )
                    """,
                    [*ids, *url_md5_list],
                )
                ids_to_delete = [row['id'] for row in cur.fetchall()]

                if not ids_to_delete:
                    conn.commit()
                    return 0

                delete_placeholders = ', '.join(['%s'] * len(ids_to_delete))

                cur.execute(
                    f"""
                    DELETE FROM pages
                    WHERE id IN ({delete_placeholders})
                    """,
                    ids_to_delete,
                )

                deleted = cur.rowcount

            conn.commit()
            return int(deleted)

        except Exception:
            conn.rollback()
            raise
