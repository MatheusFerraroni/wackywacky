"""Print decompressed text previews from pages.text."""

import logging

from miner.db import close_connection, get_connection
from miner.models.page import _decompress_str  # pylint: disable=protected-access


logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s | %(levelname)s | %(name)s | %(message)s',
)
logger = logging.getLogger(__name__)


def preview_page_text(limit: int = 10) -> None:
    """Fetch and print decompressed page text previews."""
    conn = get_connection()
    try:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT id, url, status, OCTET_LENGTH(`text`) AS compressed_bytes, `text`
                FROM pages
                WHERE `text` IS NOT NULL
                AND status = 'done'
                ORDER BY id
                LIMIT %s
                """,
                (limit,),
            )
            rows = cursor.fetchall()

        if not rows:
            print('No pages with text found.')
            return

        for row in rows:
            text = _decompress_str(row['text']) or ''
            preview = ' '.join(text.split())
            print('=' * 100)
            print(
                f'id={row["id"]} status={row["status"]} '
                f'compressed_bytes={row["compressed_bytes"]}'
            )
            print(f'url={row["url"]}')
            print(f'text_characters={len(text)}')
            print(preview[:5000])
            print()
            print(preview[-5000:])
    finally:
        close_connection()


if __name__ == '__main__':
    preview_page_text()
