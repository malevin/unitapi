# python unitapi/test.py

import requests
from loguru import logger
import pandas as pd
import json
import jwt
from datetime import datetime
import time

def print_ans_decorator(func):
    def wrapper(*args, **kwargs):
        response = func(*args, **kwargs)
        logger.info(f"\nКод ответа: {response.status_code}\nСообщение: {response.text}")
        return response
    return wrapper

requests.get = print_ans_decorator(requests.get)
requests.post = print_ans_decorator(requests.post)
requests.delete = print_ans_decorator(requests.delete)
requests.put = print_ans_decorator(requests.put)

# def get_token():
#     auth_base = 'http://127.0.0.1:5000/api/v1/auth'
#     params = {
#         'email': 'b.igor.work21@gmail.com',
#         'password': '205393'
#     }
#     response = requests.get(auth_base, json=params)
#     return response.text

# token = get_token()

# разработчик
token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJuYW1lIjoiXHUwNDE4XHUwNDMzXHUwNDNlXHUwNDQwXHUwNDRjIFx1MDQxMVx1MDQzZVx1MDQ0MFx1MDQzZVx1MDQzNFx1MDQzOFx1MDQzZCIsInJvbGVzIjpbIlx1MDQyMFx1MDQzMFx1MDQzN1x1MDQ0MFx1MDQzMFx1MDQzMVx1MDQzZVx1MDQ0Mlx1MDQ0N1x1MDQzOFx1MDQzYSJdLCJleHAiOjE2NzA2MTY4Njd9.IlXEP3FbJcIIfwAySVuDy1O_BNvAd3pLoUzUoOBYxbw'
# Никто
# token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJuYW1lIjoiXHUwNDEyXHUwNDMwXHUwNDQxXHUwNDRmIFx1MDQxZlx1MDQ0M1x1MDQzZlx1MDQzYVx1MDQzOFx1MDQzZCIsInJvbGVzIjpbIlx1MDQxZFx1MDQzOFx1MDQzYVx1MDQ0Mlx1MDQzZSJdLCJleHAiOjE2Njk4NDE5MjF9.YsrkPbUrOodzKXP_tjyG_pGCe0r2XuqJ6vN5MLhabOk'

# raise Exception
base = 'http://127.0.0.1:5000/api/v1'

# auth_base = 'http://127.0.0.1:5000/api/v1/auth'
# base = 'http://unitapi.malevin.com/api/v1'
# auth_base = 'http://unitapi.malevin.com/auth'
# path = 'http://unitapi.malevin.com/execute_sql/clc'
# key = '89a10379-1373-4a2e-b331-0adc36157443'




headers = {
    'Token': token,
    'Content-Type': 'application/json'
}
data = """
{
    "tables_to_glue": {
        "contracts": {
            "remain_cols": ["id", "name", "number", "date", "contractors_id", "objects_id", "items_id"],
            "left_on": "contracts_id",
            "right_on": "id"
        },
        "objects": {
            "remain_cols": ["id", "name", "short_name"],
            "left_on": "contracts_objects_id",
            "right_on": "id"
        },
        "contractors": {
            "remain_cols": ["id", "name"],
            "left_on": "contracts_contractors_id",
            "right_on": "id"
        },
        "items": {
            "remain_cols": ["id", "name", "banks_items_id"],
            "left_on": "contracts_items_id",
            "right_on": "id"
        },
        "banks_items": {
            "remain_cols": ["id", "name"],
            "left_on": "items_banks_items_id",
            "right_on": "id"
        }
    },
    "filter_by": {
        "items_id": 229,
        "objects_id": 4
    }
}
"""
# params = {
#     'query': [
#         '''ALTER TABLE work_types
#         DROP FOREIGN KEY work_types_ibfk_1;''',
#         'ALTER TABLE work_types DROP COLUMN items_id;'
#     ]
# }

# , data=data.encode('utf-8')
# response = requests.post(base + '/clc/production/execute_sql', headers=headers, data=params)
# response = requests.get(base + '/clc/production/special/est_mats', headers=headers, json=params)
# /api/v1/clc/<db>/actions/<action_name>
response = requests.post(base + '/uu/spv/expanded/acts', headers=headers, data=data.encode('utf-8'))
# response = requests.get(base + '/uu/spv/initial/objects', headers=headers, json={})
# logger.
# params = {
#     'email': 'b.igor.work21@gmail.com',
#     'password': '205393'
# }

# response = requests.get(base + '/uu/scandia/initial/objects', headers=headers, json={})
# response = requests.get(auth_base, headers={}, json=params)

ans = response.json()
df = pd.DataFrame(ans)
logger.debug(df)


