from miner.models import Domain, Page
from miner.enums.page_status import PageStatus
from miner.models.utils import normalize_url, md5_bin16

class Pager:
    def __init__(self, url: str, parent: "Pager | None" = None):
        self.url = url
        self.parent = parent
        self.parent_page_id = None
        self.page_recursion_level = 0

        self.page: Page | None = None

        if self.parent is not None:
            self.domain = Domain.get_or_create(self.url, self.parent.domain.recursion_level)
        else:
            self.domain = Domain.get_or_create(self.url)

        if self.parent and self.parent.page:
            self.parent_page_id = self.parent.page.id
            self.page_recursion_level = self.parent.page.recursion_level + 1

    def save(self, status: PageStatus = PageStatus.TODO) -> "Pager":
        self.page = Page.get_or_create(
            domain_id=self.domain.id,
            url=self.url,
            parent_page_id=self.parent_page_id,
            recursion_level=self.page_recursion_level,
            status=PageStatus.TODO,
        )

        return self

    def load(self) -> "Pager | None":
        url = normalize_url(self.url)
        url_md5 = md5_bin16(url)

        page = Page.get_by_md5(url_md5)
        if not page:
            return None

        domain = Domain.get_by_id(page.domain_id)

        self.page = page
        self.domain = domain

        return self
