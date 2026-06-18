"""Database-backed runtime settings cache."""

import json

from miner.db import get_connection


class SettingsDB:  # pylint: disable=too-few-public-methods
    """Read JSON settings from the database with a small local cache."""

    def __init__(self):
        """Initialize the settings cache."""
        self.configs = {}

    def get_config(self, config_name: str, refresh: bool = False):
        """Return a configuration value by name."""
        if not refresh and config_name in self.configs:
            return self.configs[config_name]

        conn = get_connection()
        with conn.cursor() as cursor:
            cursor.execute(
                'SELECT value FROM settings WHERE `key` = %s',
                (config_name,),
            )
            row = cursor.fetchone()

        if row is None:
            return None

        value = row['value']
        if isinstance(value, str):
            value = json.loads(value)

        self.configs[config_name] = value
        return value
