"""Application configuration and environment variable loading."""

import logging
import os
from pathlib import Path

from dotenv import load_dotenv

logger = logging.getLogger('config')

BASE_DIR = Path(__file__).resolve().parent.parent.parent

env_path = BASE_DIR / '.env'


if env_path.exists():
    load_dotenv(env_path)
    logger.info('Loaded environment variables from %s', env_path)
else:
    logger.info('.env file not found, using system environment variables')


def env_bool(name: str, default: bool) -> bool:
    """Parse a boolean environment variable."""
    raw = os.getenv(name)
    if raw is None:
        return default

    return raw.strip().lower() in {'true', '1', 'yes', 'on'}


class Settings:  # pylint: disable=too-few-public-methods
    """Central configuration loaded from environment variables."""

    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', '3306'))
    DB_USER = os.getenv('DB_USER', 'appuser')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'apppass')
    DB_NAME = os.getenv('DB_NAME', 'appdb')
    DATABASE_URL = os.getenv('DATABASE_URL', 'mysql+pymysql://appuser:apppass@mysql:3306/appdb')

    SECONDS_BETWEEN_CLEAN_DB = int(os.getenv('SECONDS_BETWEEN_CLEAN_DB', '60'))
    PROCESSING_TIMEOUT_SECONDS = int(os.getenv('PROCESSING_TIMEOUT_SECONDS', '900'))
    GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS = int(
        os.getenv('GRACEFUL_SHUTDOWN_TIMEOUT_SECONDS', '30')
    )
    BROWSER_BACKEND = os.getenv('BROWSER_BACKEND', 'obscura').lower()
    OBSCURA_CDP_ENDPOINT = os.getenv('OBSCURA_CDP_ENDPOINT', 'http://localhost:9222')
    OBSCURA_CDP_CONNECT_TIMEOUT_SECONDS = int(
        os.getenv('OBSCURA_CDP_CONNECT_TIMEOUT_SECONDS', '60')
    )

    OTEL_SERVICE_NAME = os.getenv('OTEL_SERVICE_NAME', 'miner')
    OTEL_SERVICE_VERSION = os.getenv('OTEL_SERVICE_VERSION', '1.0.0')
    OTEL_ENV = os.getenv('OTEL_ENV', 'dev')
    MINER_TELEMETRY_ENABLED = env_bool('MINER_TELEMETRY_ENABLED', True)
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = os.getenv(
        'OTEL_EXPORTER_OTLP_TRACES_ENDPOINT', 'http://localhost:4318'
    )
    OTEL_EXPORTER_OTLP_LOGS_ENDPOINT = os.getenv(
        'OTEL_EXPORTER_OTLP_LOGS_ENDPOINT', 'http://localhost:4318'
    )
    OTEL_EXPORTER_OTLP_METRICS_ENDPOINT = os.getenv(
        'OTEL_EXPORTER_OTLP_METRICS_ENDPOINT', 'http://localhost:4318'
    )

    LANGUAGE_TARGETS = set(['pt'])

    MAX_THREADS = int(os.getenv('MAX_THREADS', '1'))
    SECONDS_BETWEEN_LOG_THREADS = 10

    SAVE_HTML = os.getenv('SAVE_HTML', 'False').lower() == 'true'
    MAX_CHARACTERS_TEXT = int(os.getenv('MAX_CHARACTERS_TEXT', '1000000'))
    PREFLIGHT_ENABLED = env_bool('PREFLIGHT_ENABLED', True)
    PREFLIGHT_TIMEOUT_MS = int(os.getenv('PREFLIGHT_TIMEOUT_MS', '2000'))

    SECONDS_BETWEEN_UPDATE_SYSTEM_STATUS = 60


settings = Settings()
