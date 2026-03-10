"""
Pager abstraction responsible for linking URLs, pages and domains.

This class encapsulates the logic used to:
- resolve the domain for a URL
- determine recursion level based on the parent page
- persist or load a Page object from the database
"""

from miner.models import Domain, Page
from miner.enums.page_status import PageStatus
from miner.models.utils import normalize_url, md5_bin16


class Pager:
    """Represents a page being processed in the crawler pipeline."""

    def __init__(self, url: str, parent: 'Pager | None' = None):
        """
        Initialize a Pager instance.

        Args:
            url: URL of the page to be processed.
            parent: Optional parent Pager that discovered this URL.
        """
        self.url = url
        self.parent = parent
        self.parent_page_id = None
        self.page_recursion_level = 0

        self.page: Page | None = None

        self.domain = Domain.get_or_create(self.url, parent_pager=self.parent)

        if self.parent and self.parent.page:
            self.parent_page_id = self.parent.page.id

            if self.domain.id == self.parent.domain.id:
                self.page_recursion_level = self.parent.page.recursion_level + 1
            else:
                self.page_recursion_level = 0

    def save(self, status: PageStatus = PageStatus.TODO) -> 'Pager':
        """
        Persist the page in the database, creating it if necessary.

        Args:
            status: Initial status for the page.

        Returns:
            The Pager instance with the associated Page loaded.
        """
        self.page = Page.get_or_create(
            domain_id=self.domain.id,
            url=self.url,
            parent_page_id=self.parent_page_id,
            recursion_level=self.page_recursion_level,
            status=status,
        )

        return self

    def load(self) -> 'Pager | None':
        """
        Load an existing page from the database using the URL.

        Returns:
            The Pager instance if the page exists, otherwise None.
        """
        url = normalize_url(self.url)
        url_md5 = md5_bin16(url)

        page = Page.get_by_md5(url_md5)
        if not page:
            return None

        domain = Domain.get_by_id(page.domain_id)

        self.page = page
        self.domain = domain

        return self
