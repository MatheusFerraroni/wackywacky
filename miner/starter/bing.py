"""
Starter implementation for generating Bing search URLs.
"""

from miner.starter.starter_default import StarterDefault


class StarterBing(StarterDefault):  # pylint: disable=too-few-public-methods
    """Starter responsible for creating Bing search queries."""

    BASE_URL = 'https://www.bing.com/search'
