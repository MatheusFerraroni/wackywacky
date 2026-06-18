"""Load a page and its domain before handing work to Requester."""

from miner.models import Domain, Page


class Pager:  # pylint: disable=too-few-public-methods
    """Container for the current page/domain pair being mined."""

    def __init__(self, page_id: int):
        """Initialize a pager for a page id."""
        self.page_id = page_id

        self.page = None
        self.domain = None
        self.url = None
        self.page_recursion_level = None

    def load(self) -> bool:
        """Load page and domain records from the database."""
        self.page = Page.get_by_id(self.page_id)
        if not self.page:
            raise LookupError(f'Page with id {self.page_id} not found')

        self.domain = Domain.get_by_id(self.page.domain_id)
        self.url = self.page.url
        self.page_recursion_level = self.page.recursion_level

        return True
