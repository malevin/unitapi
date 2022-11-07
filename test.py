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
    'stage': 'development'
}

params = {
    # 'id': 1140,
    'inn': '2364624624633463246245624572236',
    'name': 'TEST100',
    'taxation_type': 34262,
    'is_active': True
}

response = requests.put(base + 'contractors', json=params, headers=headers)

input()
response = requests.get(base + 'contractors', json={}, headers=headers)

ans = response.json()
df = pd.DataFrame(ans['data'])
# logger.debug(ans.text)

# inserted_id = df['id'].loc[0]

# input()

# params = {

# }
# response = requests.get(base + 'contractors', json=params, headers=headers) # , verify=False

# ans = response.json()
# df = pd.DataFrame(ans['data'])
logger.debug(df)





# params = {
#     'id': int(inserted_id),
# }
# ans = requests.delete(base + 'contractors', json=params)

