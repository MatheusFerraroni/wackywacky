from miner.db import get_connection
import json


class SettingsDB:
    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(SettingsDB, cls).__new__(cls)
            cls._instance.con = get_connection()
            cls._instance.configs = {}
        return cls._instance

    def get_config(self, config_name: str, refresh=False):

        if not refresh and config_name in self.configs:
            return self.configs[config_name]

        conn = self.con
        with conn.cursor() as cursor:
            cursor.execute(
                "SELECT value FROM settings WHERE `key` = %s",
                (config_name,),
            )
            row = cursor.fetchone()

        if row is None:
            return None

        self.configs[config_name] = json.loads(row["value"])
        return self.configs[config_name]
