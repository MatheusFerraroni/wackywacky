from enum import Enum


class PageStatus(Enum):
    TODO                    = 'todo'
    PROCESSING              = 'processing'
    DONE                    = 'done'
    FAILED                  = 'failed'
    DOMAIN_BLOCKED          = 'blocked_domain'
    BLOCKED_LIMIT_RECURSION  = 'blocked_limit_recursion'

    def __str__(self) -> str:
        return self.value
