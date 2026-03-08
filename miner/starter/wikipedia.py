from miner.starter.starter_default import StarterDefault


class StarterWikipedia(StarterDefault):
    QUERY_PARAM = 'search'
    BASE_URL = "http://pt.wikipedia.org/w/index.php?fulltext=1&"
