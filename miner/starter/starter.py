"""
Starter manager responsible for generating the initial URLs
used by the crawler based on configured search engines and terms.
"""

import logging
from typing import Set

from miner.settings.settings_db import SettingsDB
from miner.starter.bing import StarterBing
from miner.starter.duckduckgo import StarterDuckDuckGo
from miner.starter.google import StarterGoogle
from miner.starter.wikipedia import StarterWikipedia


class Starter:  # pylint: disable=too-few-public-methods
    """Coordinates the enabled search engines to produce initial crawl URLs."""

    def __init__(self) -> None:
        """Initialize enabled starter engines based on configuration."""
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info('Starter created')
        self.engines = [
            StarterGoogle(),
            StarterDuckDuckGo(),
            StarterBing(),
            StarterWikipedia(),
        ]

        engines_to_active = set(SettingsDB().get_config('search_engine'))

        self.engines = list(
            filter(lambda x: x.__class__.__name__ in engines_to_active, self.engines)
        )
        self.logger.info('Total starters actives: %s', len(self.engines))
        for engine in self.engines:
            self.logger.info('Name %s', engine.__class__.__name__)

    def get_init_urls(self) -> Set[str]:
        """
        Generate the set of initial URLs to begin crawling.

        Returns:
            set[str]: Unique URLs generated from all enabled engines
            using the configured initial search terms.
        """
        self.logger.info('Running starters')
        init_terms = SettingsDB().get_config('init_terms')

        self.logger.info('Total terms to start: %s', len(init_terms))

        urls_to_init = set()
        for engine in self.engines:
            for term in init_terms:
                urls_to_init.add(engine.create_query(term))

        self.logger.info('Total start urls: %s', len(urls_to_init))

        return urls_to_init
