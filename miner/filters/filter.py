from functools import lru_cache
import logging
from miner.models.utils import extract_hostname, md5_bin16
from miner.models.blocked_domain import BlockedDomain
from miner.settings.settings import Settings
from langdetect import detect
from miner.metrics import metric_domain_check_duration
import time

logger = logging.getLogger(__name__)


def is_domain_blocked(url_or_domain: str) -> bool:
    start_timer = time.perf_counter()
    host = extract_hostname(url_or_domain)
    if not host:
        return False

    ret = _is_domain_blocked_cached(host)

    metric_domain_check_duration.record((time.perf_counter() - start_timer), {'service': 'miner'})
    return ret


@lru_cache(maxsize=10_000)
def _is_domain_blocked_cached(host: str) -> bool:
    domain_md5 = md5_bin16(host)
    return BlockedDomain.get_by_md5(domain_md5) is not None


def detect_lang(text: str) -> bool:
    try:
        lang = detect(text)
        return lang in Settings.LANGUAGE_TARGETS
    except Exception:
        logger.info('Failed to detect language')
        return False
