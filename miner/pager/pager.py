from miner.models import Domain, Page


class Pager:
    def __init__(self, page_id: int):
        self.page_id = page_id

        self.page = None
        self.domain = None
        self.url = None
        self.page_recursion_level = None

    def load(self) -> bool:
        self.page = Page.get_by_id(self.page_id)
        if not self.page:
            raise Exception(f'Page with id {self.page_id} not found')

        self.domain = Domain.get_by_id(self.page.domain_id)
        self.url = self.page.url
        self.page_recursion_level = self.page.recursion_level

        return True
