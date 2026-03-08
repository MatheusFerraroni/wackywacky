# from miner.db import get_connection
# import json


# def get_config(config_name: str):
#     conn = get_connection()
#     with conn.cursor() as cursor:
#         cursor.execute(
#             "SELECT value FROM settings WHERE `key` = %s",
#             (config_name,),
#         )
#         row = cursor.fetchone()
#     if row is None:
#         return None
#     return json.loads(row["value"])
