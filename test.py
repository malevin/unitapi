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

# params = {
#   'name': 'Тестовый3',
#   'taxation_type': 'НДС',
#   'contacts': '8235235',
# #   'is_active': 1,
# }

# ans = requests.put(base + 'contractors', json=params)

# input()
params = {
    'name': 'Тест',
    # 'asbd': 'AEBAFB'
    # 'name': 'Тестовый3',
}
headers = {
    'key': key,
    # "Content-Type": "application/json"
}
response = requests.get(base + '/contractors', json=params, headers=headers) # , verify=False

ans = response.json()
df = pd.DataFrame(ans['data'])
logger.debug(df)

# inserted_id = df['id'].loc[0]

input()

response = requests.get(base + '/contractors', json=params, headers=headers) # , verify=False

ans = response.json()
df = pd.DataFrame(ans['data'])
logger.debug(df)
# params = {
#     'id': int(inserted_id),
# }
# ans = requests.delete(base + 'contractors', json=params)

