"""URL and hashing helpers for model classes."""

import hashlib
from urllib.parse import urlparse

WEB_SCHEMES = {'http', 'https'}
BLOCKED_SCHEMES = {
    'about',
    'blob',
    'chrome',
    'data',
    'file',
    'ftp',
    'javascript',
    'mailto',
    'tel',
}


def md5_bin16(value: str) -> bytes:
    """Return the 16-byte MD5 digest for a string."""
    if value is None:
        raise ValueError('Cannot hash None value')
    return hashlib.md5(value.encode('utf-8')).digest()


def md5_hex(value: str) -> str:
    """Return the hexadecimal MD5 digest for a string."""
    if value is None:
        raise ValueError('Cannot hash None value')
    return hashlib.md5(value.encode('utf-8')).hexdigest()


def extract_hostname(url: str) -> str:
    """Normalize a URL and return its hostname without a www prefix."""
    if not url:
        raise ValueError('URL cannot be empty')

    url = normalize_url(url)

    parsed = urlparse(url)
    host = (parsed.hostname or '').lower()

    if host.startswith('www.'):
        host = host[4:]

    return host


def is_valid_url(url: str) -> bool:
    """Return whether a value can be normalized into a URL with a host."""
    try:
        url = normalize_url(url)

        r = urlparse(url)

        if r.scheme not in WEB_SCHEMES:
            return False

        if not r.netloc:
            return False

        host = r.hostname
        if host is None:
            return False

        _ = r.port

        return True

    except (TypeError, ValueError):
        return False


def normalize_url(url: str) -> str:
    """Normalize a URL by adding a scheme, stripping fragments, and trimming slashes."""
    url = url[:8191]
    if not url:
        raise ValueError('URL cannot be empty')

    url = url.strip().split('#')[0]
    if not url:
        raise ValueError('URL cannot be empty')

    parsed = urlparse(url)
    scheme = parsed.scheme.lower()
    if scheme in BLOCKED_SCHEMES:
        raise ValueError(f'Unsupported URL scheme: {scheme}')

    if '://' in url:
        if scheme not in WEB_SCHEMES:
            raise ValueError(f'Unsupported URL scheme: {scheme}')
    else:
        url = 'http://' + url

    while url.endswith('/'):
        url = url[:-1]
    return url
