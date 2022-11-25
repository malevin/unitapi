# python unitapi/test.py

import requests
from loguru import logger
import pandas as pd
import json

# logger.debug(entities)

# raise Exception
base = 'http://127.0.0.1:5000/clc/api/v1/'
auth_base = 'http://127.0.0.1:5000/auth'
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
    # 'name': 'admin',
    # 'pin_code': 'admin_pwd',
    # # 'Content-Type': 'application/json'
}

params = {
    'name': 'admin',
    'pin_code': 'admin_pwd',
}

# data = """
# {
#     "tables_to_glue": {
#         "contracts": {
#             "remain_cols": ["id","name", "number", "date"],
#             "left_on": "contracts_id",
#             "right_on": "id"
#         }
#     },
#     "filter_by": {
#         "estimation_id": 1
#     }
# }
# """

# response = requests.post(base + 'special/ek_mats', data=data.encode('utf-8'), headers=headers)
response = requests.get(auth_base, headers=headers, json=params)

# input()
# response = requests.get(base + 'contractors', json={}, headers=headers)

ans = response.json()
# logger.debug(type(ans['data']))
# logger.debug(ans)
# logger.debug(type(ans))
df = pd.DataFrame(ans)
logger.debug(df)
logger.debug(df.columns)

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

# {
#     "tables_to_glue": {
#         "objects": {
#             "remain_cols": ["id", "name"],
#             "left_on": "objects_id",
#             "right_on": "id"
#         }
#     }
# }


