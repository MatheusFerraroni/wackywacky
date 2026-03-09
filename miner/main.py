import logging
import sys


def configure_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s | %(levelname)s | %(name)s | %(threadName)s => %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.StreamHandler(sys.stdout)],
    )


def main() -> int:
    configure_logging(level=logging.INFO)

    from miner.telemetry import setup_telemetry
    setup_telemetry()

    from miner.app import App

    logger = logging.getLogger(__name__)
    logger.info("Starting main")

    app = App()
    return app.run()


if __name__ == "__main__":
    raise SystemExit(main())