"""
Base starter implementation used by search engine starters to
generate query URLs for the crawler.
"""

from abc import ABC
from urllib.parse import quote_plus


class StarterDefault(ABC):  # pylint: disable=too-few-public-methods
    """
    Abstract base class for search engine starters.

    Provides a common implementation to build search URLs using
    a base URL and query parameter defined by subclasses.
    """

    BASE_URL = None
    QUERY_PARAM = 'q'

    def create_query(self, starter_query: str) -> str:
        """
        Build a search query URL for the configured search engine.

        Args:
            starter_query (str): The search term used to generate the query URL.

        Returns:
            str: Fully formatted search URL.

        Raises:
            NotImplementedError: If BASE_URL is not defined by the subclass.
            TypeError: If the provided query is not a string.
            ValueError: If the query string is empty after trimming.
        """
        if not self.BASE_URL:
            raise NotImplementedError('Subclasses must define BASE_URL')

        if not isinstance(starter_query, str):
            raise TypeError('starter_query must be a string')

        query = starter_query.strip()
        if not query:
            raise ValueError('starter_query cannot be empty')

        encoded_query = quote_plus(query)
        separator = '&' if '?' in self.BASE_URL else '?'
        return f'{self.BASE_URL}{separator}{self.QUERY_PARAM}={encoded_query}'
