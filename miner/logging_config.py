"""Shared stdout logging configuration."""

import logging
import sys

SUCCESS_LEVEL = 25
logging.addLevelName(SUCCESS_LEVEL, 'SUCCESS')


def success(self, message, *args, **kwargs):
    """Log a message with the custom SUCCESS level."""
    if self.isEnabledFor(SUCCESS_LEVEL):
        # pylint: disable=protected-access
        self._log(SUCCESS_LEVEL, message, args, **kwargs)


logging.Logger.success = success


class ColoredFormatter(logging.Formatter):
    """Formatter that colorizes selected log levels for stdout."""

    COLORS = {
        SUCCESS_LEVEL: '\033[32m',
        logging.WARNING: '\033[33m',
        logging.ERROR: '\033[31m',
        logging.CRITICAL: '\033[41m',
    }

    RESET = '\033[0m'

    def format(self, record):
        """Format the record with ANSI color when configured."""
        message = super().format(record)
        color = self.COLORS.get(record.levelno)

        if color:
            return f'{color}{message}{self.RESET}'

        return message


def configure_logging(level: int = logging.INFO) -> None:
    """Configure stdout logging for the miner process."""
    handler = logging.StreamHandler(sys.stdout)

    formatter = ColoredFormatter(
        '%(asctime)s | %(levelname)s | %(name)s | %(threadName)s => %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
    )

    handler.setFormatter(formatter)

    root = logging.getLogger()
    root.setLevel(level)
    root.handlers.clear()
    root.addHandler(handler)
