"""Blocked domain model."""

from dataclasses import dataclass
from datetime import datetime

from miner.db import get_connection
from miner.models.utils import extract_hostname, md5_bin16


@dataclass
class BlockedDomain:
    """Domain that should not be crawled."""

    id: int | None
    domain: str
    domain_md5: bytes
    reason: str | None = None

    created_at: datetime | None = None
    updated_at: datetime | None = None

    @classmethod
    def from_domain(cls, any_url_or_domain: str, reason: str | None = None) -> 'BlockedDomain':
        """Build a blocked-domain instance from a URL or hostname."""
        dom = extract_hostname(any_url_or_domain)
        return cls(
            id=None,
            domain=dom,
            domain_md5=md5_bin16(dom),
            reason=reason,
        )

    @classmethod
    def get_by_md5(cls, domain_md5: bytes) -> 'BlockedDomain | None':
        """Load a blocked domain by MD5 digest."""
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    domain,
                    domain_md5,
                    reason,
                    created_at,
                    updated_at
                FROM blocked_domain
                WHERE domain_md5=%s
                LIMIT 1
                """,
                (domain_md5,),
            )
            row = cur.fetchone()
            return cls(**row) if row else None

    @classmethod
    def get_by_domain(cls, domain: str) -> 'BlockedDomain | None':
        """Load a blocked domain by URL or hostname."""
        dom = extract_hostname(domain)
        dom_md5 = md5_bin16(dom)
        return cls.get_by_md5(dom_md5)

    @classmethod
    def get_by_id(cls, blocked_id: int) -> 'BlockedDomain | None':
        """Load a blocked domain by primary key."""
        conn = get_connection()
        with conn.cursor() as cur:
            cur.execute(
                """
                SELECT
                    id,
                    domain,
                    domain_md5,
                    reason,
                    created_at,
                    updated_at
                FROM blocked_domain
                WHERE id=%s
                LIMIT 1
                """,
                (blocked_id,),
            )
            row = cur.fetchone()
            return cls(**row) if row else None
