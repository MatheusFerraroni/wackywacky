from enum import Enum


class PageStatus(Enum):
    TODO = 'todo'
    PROCESSING = 'processing'
    DONE = 'done'
    FAILED = 'failed'
    FAILED_TIMEOUT = 'failed_timeout'
    DOMAIN_BLOCKED = 'blocked_domain'
    BLOCKED_LIMIT_RECURSION = 'blocked_limit_recursion'
    BLOCKED_LANGUAGE = 'blocked_language'

    def __str__(self) -> str:
        return self.value
