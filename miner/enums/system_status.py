"""System status enum."""

from enum import Enum


class SystemStatus(Enum):
    """Global states that drive the miner runtime."""

    STARTING = 'starting'
    RUNNING_STARTER = 'running_starter'
    RUNNING_MINING = 'running_mining'
    COMPLETED = 'completed'
    ERROR = 'error'
    STOPPING = 'stopping'

    def __str__(self) -> str:
        return self.value
