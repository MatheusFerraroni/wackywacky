import os
from pathlib import Path
from dotenv import load_dotenv
import logging
import json


logger = logging.getLogger('config')

BASE_DIR = Path(__file__).resolve().parent.parent.parent

env_path = BASE_DIR / '.env'


if env_path.exists():
    load_dotenv(env_path)
    logger.info('Loaded environment variables from %s', env_path)
else:
    logger.info('.env file not found, using system environment variables')


class Settings:
    DB_HOST = os.getenv('DB_HOST', 'localhost')
    DB_PORT = int(os.getenv('DB_PORT', 3306))
    DB_USER = os.getenv('DB_USER', 'appuser')
    DB_PASSWORD = os.getenv('DB_PASSWORD', 'apppass')
    DB_NAME = os.getenv('DB_NAME', 'appdb')
    DATABASE_URL = os.getenv('DATABASE_URL', 'mysql+pymysql://appuser:apppass@mysql:3306/appdb')

    SECONDS_BETWEEN_CLEAN_DB = os.getenv('SECONDS_BETWEEN_CLEAN_DB', 60)

    OTEL_SERVICE_NAME = os.getenv('OTEL_SERVICE_NAME', 'miner')
    OTEL_SERVICE_VERSION = os.getenv('OTEL_SERVICE_VERSION', '1.0.0')
    OTEL_ENV = os.getenv('OTEL_ENV', 'dev')
    OTEL_EXPORTER_OTLP_TRACES_ENDPOINT = os.getenv('OTEL_EXPORTER_OTLP_TRACES_ENDPOINT', 'http://localhost:4318')
    OTEL_EXPORTER_OTLP_LOGS_ENDPOINT = os.getenv('OTEL_EXPORTER_OTLP_LOGS_ENDPOINT', 'http://localhost:4318')
    OTEL_EXPORTER_OTLP_METRICS_ENDPOINT = os.getenv('OTEL_EXPORTER_OTLP_METRICS_ENDPOINT', 'http://localhost:4318')

    LANGUAGE_TARGETS = set(['pt'])

    MAX_THREADS = int(os.getenv('MAX_THREADS', 1))
    SECONDS_BETWEEN_LOG_THREADS = 10

settings = Settings()
