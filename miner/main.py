"""Command-line entry point for the miner process."""

import argparse
import logging

from miner.logging_config import configure_logging


def main() -> int:
    """Parse CLI arguments and run the miner application."""
    configure_logging(level=logging.INFO)

    # pylint: disable=import-outside-toplevel
    from miner.telemetry import setup_telemetry

    setup_telemetry()

    parser = argparse.ArgumentParser()
    parser.add_argument('--reset-db', action='store_true', help='Reset the database')

    args = parser.parse_args()

    # pylint: disable=import-outside-toplevel
    from miner.app import App

    logger = logging.getLogger(__name__)
    logger.info('Starting main')

    app = App(args.reset_db)
    return app.run()


if __name__ == '__main__':
    raise SystemExit(main())
