"""
Starter implementation for generating Google search URLs.
"""

from miner.starter.starter_default import StarterDefault


class StarterGoogle(StarterDefault):  # pylint: disable=too-few-public-methods
    """Starter responsible for creating Google search queries."""

    BASE_URL = 'https://www.google.com/search'
