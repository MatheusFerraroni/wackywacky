"""
Starter implementation for generating Duckduckgo search URLs.
"""

from miner.starter.starter_default import StarterDefault


class StarterDuckDuckGo(StarterDefault):  # pylint: disable=too-few-public-methods
    """Starter responsible for creating Duckduckgo search queries."""

    BASE_URL = 'https://duckduckgo.com/'
