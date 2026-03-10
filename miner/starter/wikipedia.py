"""
Wikipedia starter implementation.

Provides configuration for generating search URLs using the
Wikipedia search endpoint.
"""

from miner.starter.starter_default import StarterDefault


class StarterWikipedia(StarterDefault):  # pylint: disable=too-few-public-methods
    """Starter configuration for crawling search results from Wikipedia."""

    QUERY_PARAM = 'search'
    BASE_URL = 'http://pt.wikipedia.org/w/index.php?fulltext=1&'
