import logging
import sys
import argparse


SUCCESS_LEVEL = 25
logging.addLevelName(SUCCESS_LEVEL, 'SUCCESS')


def success(self, message, *args, **kwargs):
    if self.isEnabledFor(SUCCESS_LEVEL):
        self._log(SUCCESS_LEVEL, message, args, **kwargs)


logging.Logger.success = success


class ColoredFormatter(logging.Formatter):
    COLORS = {
        SUCCESS_LEVEL: '\033[32m',
        logging.WARNING: '\033[33m',
        logging.ERROR: '\033[31m',
        logging.CRITICAL: '\033[41m',
    }

    RESET = '\033[0m'

    def format(self, record):
        message = super().format(record)
        color = self.COLORS.get(record.levelno)

        if color:
            return f'{color}{message}{self.RESET}'

        return message


def configure_logging(level: int = logging.INFO) -> None:
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


def main() -> int:
    configure_logging(level=logging.INFO)

    from miner.telemetry import setup_telemetry

    setup_telemetry()

    parser = argparse.ArgumentParser()
    parser.add_argument('--reset-db', action='store_true', help='Reset the database')

    args = parser.parse_args()

    from miner.app import App

    logger = logging.getLogger(__name__)
    logger.info('Starting main')

    app = App(args.reset_db)
    return app.run()


if __name__ == '__main__':
    raise SystemExit(main())
