# python unitapi/test.py

import requests
from loguru import logger
import pandas as pd
import json

base = 'http://127.0.0.1:5000/clc/api/v1/'
# base = 'http://unitapi.malevin.com/'
key = '89a10379-1373-4a2e-b331-0adc36157443'
# key = '89a10379-1373-4a2e-b336157443'

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

headers = {
    'key': key,
    'stage': 'production',
    'Content-Type': 'application/json'
}

params = {
    # 'name': '2352',
    'surname': 'Borodin',
    # 'name': 'TEST100',
    # 'taxation_type': 34262,
    # 'is_active': True
}
data = """
{
    "tables_to_glue": {
        "clc": {
            "remain_cols": ["id", "name", "contracts_id"],
            "left_on": "clc_id",
            "right_on": "id"
        },
        "contracts": {
            "remain_cols": ["id", "name", "items_id"],
            "left_on": "clc_contracts_id",
            "right_on": "id"
        },
        "items": {
            "remain_cols": ["id", "name", "sub_pakets_id"],
            "left_on": "contracts_items_id",
            "right_on": "id"
        },
        "sub_pakets": {
            "remain_cols": ["id", "name"],
            "left_on": "items_sub_pakets_id",
            "right_on": "id"
        }
    }
}
"""

response = requests.post(base + 'expanded/ek', data=data.encode('utf-8'), headers=headers)
# response = requests.get(base + '', headers=headers, json={})

# input()
# response = requests.get(base + 'contractors', json={}, headers=headers)

ans = response.json()
# logger.debug(type(ans['data']))
df = pd.DataFrame(ans['data'])
logger.debug(df)

# inserted_id = df['id'].loc[0]

# input()

# params = {

# }
# response = requests.get(base + 'contractors', json=params, headers=headers) # , verify=False

# ans = response.json()
# df = pd.DataFrame(ans['data'])
# logger.debug(df)





# params = {
#     'id': int(inserted_id),
# }
# ans = requests.delete(base + 'contractors', json=params)

