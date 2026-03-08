from abc import ABC
from urllib.parse import quote_plus


class StarterDefault(ABC):
    BASE_URL = None
    QUERY_PARAM = "q"

    def create_query(self, starter_query: str) -> str:
        if not self.BASE_URL:
            raise NotImplementedError("Subclasses must define BASE_URL")

        if not isinstance(starter_query, str):
            raise TypeError("starter_query must be a string")

        query = starter_query.strip()
        if not query:
            raise ValueError("starter_query cannot be empty")

        encoded_query = quote_plus(query)
        separator = "&" if "?" in self.BASE_URL else "?"
        return f"{self.BASE_URL}{separator}{self.QUERY_PARAM}={encoded_query}"