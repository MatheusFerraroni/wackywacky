from functools import lru_cache

from miner.models.utils import extract_hostname, md5_bin16
from miner.models.blocked_domain import BlockedDomain


def is_domain_blocked(url_or_domain: str) -> bool:
    host = extract_hostname(url_or_domain)
    if not host:
        return False
    return _is_domain_blocked_cached(host)


@lru_cache(maxsize=10_000)
def _is_domain_blocked_cached(host: str) -> bool:
    domain_md5 = md5_bin16(host)
    return BlockedDomain.get_by_md5(domain_md5) is not None
