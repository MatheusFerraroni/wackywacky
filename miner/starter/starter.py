from miner.starter.google import StarterGoogle
from miner.starter.duckduckgo import StarterDuckDuckGo
from miner.starter.bing import StarterBing
from miner.starter.wikipedia import StarterWikipedia
from miner.settings.settings_db import SettingsDB
import logging

class Starter:
    def __init__(self):
        self.logger = logging.getLogger(self.__class__.__name__)
        self.logger.info('Starter created')
        self.engines = [StarterGoogle(), StarterDuckDuckGo(), StarterBing(), StarterWikipedia()]
        
        engines_to_active = set(SettingsDB().get_config('search_engine'))

        self.engines = list(
            filter(
                lambda x: x.__class__.__name__ in engines_to_active, self.engines
            )
        )
        self.logger.info(f'Total starters actives: {len(self.engines)}')
        for engine in self.engines:
            self.logger.info(f'Name {engine.__class__.__name__}')


    def get_init_urls(self):
        self.logger.info('Running starters')
        init_terms = SettingsDB().get_config('init_terms')

        self.logger.info(f'Total terms to start: {len(init_terms)}')

        urls_to_init = set()
        for engine in self.engines:
            for term in init_terms:
                urls_to_init.add(engine.create_query(term))

        self.logger.info(f'Total start urls: {len(urls_to_init)}')

        return urls_to_init
