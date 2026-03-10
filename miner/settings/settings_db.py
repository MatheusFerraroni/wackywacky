from miner.db import get_connection
import json


class SettingsDB:  # pylint: disable=too-few-public-methods
    def __init__(self):
        self.configs = {}

    def get_config(self, config_name: str, refresh: bool = False):
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
