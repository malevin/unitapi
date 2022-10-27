# python REST_API/test.py

import requests
from loguru import logger
import pandas as pd
import json


base = 'http://127.0.0.1:5000/clc/api/v1/'
# base = 'http://unitapi.malevin.com/'


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

params = {
  'name': 'Тестовый3',
  'taxation_type': 'НДС',
  'contacts': '8235235',
#   'is_active': 1,
}

ans = requests.put(base + 'contractors', json=params)

input()
params = {
    'name': 'Тестовый3',
}
response = requests.get(base + '/contractors', json=params) # , verify=False

ans = response.json()
df = pd.DataFrame(ans['data'])
inserted_id = df['id'].loc[0]

input()
params = {
    'id': int(inserted_id),
}
ans = requests.delete(base + 'contractors', json=params)

