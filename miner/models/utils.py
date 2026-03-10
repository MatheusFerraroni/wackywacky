import hashlib
from urllib.parse import urlparse


def md5_bin16(value: str) -> bytes:
    if value is None:
        raise ValueError('Cannot hash None value')
    return hashlib.md5(value.encode('utf-8')).digest()


def md5_hex(value: str) -> str:
    if value is None:
        raise ValueError('Cannot hash None value')
    return hashlib.md5(value.encode('utf-8')).hexdigest()


def extract_hostname(url: str) -> str:
    if not url:
        raise ValueError('URL cannot be empty')

    url = normalize_url(url)

    parsed = urlparse(url)
    host = (parsed.hostname or '').lower()

    if host.startswith('www.'):
        host = host[4:]

    return host


def is_valid_url(url: str) -> bool:
    try:
        url = normalize_url(url)

        r = urlparse(url)

        if not r.netloc:
            return False

        host = r.hostname
        if host is None:
            return False

        return True

    except Exception:
        return False


def normalize_url(url: str) -> str:
    url = url[:8191]
    if not url:
        raise ValueError('URL cannot be empty')

    url = url.strip().split('#')[0]

    if '://' not in url:
        url = 'http://' + url

    while url.endswith('/'):
        url = url[:-1]
    return url
