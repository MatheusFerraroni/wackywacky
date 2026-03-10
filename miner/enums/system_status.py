from enum import Enum


class SystemStatus(Enum):
    STARTING = 'starting'
    RUNNING_STARTER = 'running_starter'
    RUNNING_MINING = 'running_mining'
    COMPLETED = 'completed'
    ERROR = 'error'
    STOPPING = 'stopping'

    def __str__(self) -> str:
        return self.value
