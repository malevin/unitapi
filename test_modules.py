# Несколько модулей, с которыми удобно тестировать API вручную

from loguru import logger
import pandas as pd
import requests
import jwt
from datetime import datetime



token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoiXHUwNDE4XHUwNDMzXHUwNDNlXHUwNDQwXHUwNDRjIFx1MDQxMVx1MDQzZVx1MDQ0MFx1MDQzZVx1MDQzNFx1MDQzOFx1MDQzZCIsInJvbGVzIjpbIlx1MDQyMFx1MDQzMFx1MDQzN1x1MDQ0MFx1MDQzMFx1MDQzMVx1MDQzZVx1MDQ0Mlx1MDQ0N1x1MDQzOFx1MDQzYSJdLCJleHAiOjE5ODcxMzY3MzJ9.18FEpvUm56aAuQ3RvpFE-kz7NkycIIJLSn7o3Isot2E'


def convert_to_df_decorator(func):
    def wrapper(*args, **kwargs):
        try:
            response = func(*args, **kwargs)
            logger.info(f"\nКод ответа: {response.status_code}")
            df = pd.DataFrame(response.json())
            return df
        except Exception:
            logger.exception('Ошибка при получении датафрейма')
        return response
    return wrapper


def print_ans_decorator(func):
    def wrapper(*args, **kwargs):
        response = func(*args, **kwargs)
        logger.info(f"\nКод ответа: {response.status_code}\nСообщение: {response.text}")
        return response
    return wrapper

@print_ans_decorator
def print_get(*args, **kwargs):
    ans = requests.get(*args, **kwargs)
    return ans

@print_ans_decorator
def print_post(*args, **kwargs):
    ans = requests.post(*args, **kwargs)
    return ans

@print_ans_decorator
def print_put(*args, **kwargs):
    ans = requests.put(*args, **kwargs)
    return ans

@print_ans_decorator
def print_delete(*args, **kwargs):
    ans = requests.delete(*args, **kwargs)
    return ans


@convert_to_df_decorator
def df_get(*args, **kwargs):
    ans = requests.get(*args, **kwargs)
    return ans

@convert_to_df_decorator
def df_post(*args, **kwargs):
    ans = requests.post(*args, **kwargs)
    return ans


# def get_token():
#     auth_base = 'http://127.0.0.1:5000/api/v1/auth'
#     # auth_base = 'http://unitapi.malevin.com/api/v1/auth'
#     params = {
#         'email': 'b.igor.work21@gmail.com',
#         'password': '205393'
#     }
#     response = get(auth_base, json=params)
#     return response.json()
