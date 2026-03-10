import logging
import sys
import argparse


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format='%(asctime)s | %(levelname)s | %(name)s | %(threadName)s => %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S',
        handlers=[logging.StreamHandler(sys.stdout)],
    )


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
