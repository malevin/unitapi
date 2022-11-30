# python unitapi/test.py

import requests
from loguru import logger
import pandas as pd
import json
import jwt
from datetime import datetime
import time

# logger.debug(entities)


# your_timestamp = 1669643004
# date = datetime.utcfromtimestamp(your_timestamp)
# logger.debug(date)
# raise Exception


# разработчик
token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJuYW1lIjoiXHUwNDE4XHUwNDMzXHUwNDNlXHUwNDQwXHUwNDRjIFx1MDQxMVx1MDQzZVx1MDQ0MFx1MDQzZVx1MDQzNFx1MDQzOFx1MDQzZCIsInJvbGVzIjpbIlx1MDQyMFx1MDQzMFx1MDQzN1x1MDQ0MFx1MDQzMFx1MDQzMVx1MDQzZVx1MDQ0Mlx1MDQ0N1x1MDQzOFx1MDQzYSJdLCJleHAiOjE2Njk4NDA2ODh9.4q829Jw5LX7lipcJUuXgnw1FHJp1uiHbWqHMUZHm8CQ'
# Никто
# token = 'eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJuYW1lIjoiXHUwNDEyXHUwNDMwXHUwNDQxXHUwNDRmIFx1MDQxZlx1MDQ0M1x1MDQzZlx1MDQzYVx1MDQzOFx1MDQzZCIsInJvbGVzIjpbIlx1MDQxZFx1MDQzOFx1MDQzYVx1MDQ0Mlx1MDQzZSJdLCJleHAiOjE2Njk4NDE5MjF9.YsrkPbUrOodzKXP_tjyG_pGCe0r2XuqJ6vN5MLhabOk'

# raise Exception
# base = 'http://127.0.0.1:5000/clc/api/v1/'
# auth_base = 'http://127.0.0.1:5000/auth'
# base = 'http://unitapi.malevin.com/'
# auth_base = 'http://unitapi.malevin.com/auth'
path = 'http://unitapi.malevin.com/execute_sql/clc'
# key = '89a10379-1373-4a2e-b331-0adc36157443'

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
    'Token': token,
    'Stage': 'development',
    # 'Content-Type': 'application/json'
}

params = {
    'query': [
        'update * from acts',
        '''CREATE TABLE `test` (
        `test_col` varchar(255) UNIQUE NOT NULL
        )''',
        'alter table test add column surname_test varchar(255)',
        'drop table test'
    ]
}

# response = requests.post(base + 'special/ek_mats', data=data.encode('utf-8'), headers=headers)
response = requests.post(path, headers=headers, json=params)

# ans = response.json()

# logger.debug(ans[1])


