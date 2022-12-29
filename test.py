# python unitapi/test.py

import requests
from loguru import logger
import pandas as pd
import json
import jwt
from datetime import datetime
import time
from pprint import pprint
import sys
sys.path.append('/Users/igorigor/VS code/Python work scripts/')
# sys.path.append('/home/pavelmalevin/regular_loadings')
from test_modules import *

token = 'eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJuYW1lIjoiXHUwNDE4XHUwNDMzXHUwNDNlXHUwNDQwXHUwNDRjIFx1MDQxMVx1MDQzZVx1MDQ0MFx1MDQzZVx1MDQzNFx1MDQzOFx1MDQzZCIsInJvbGVzIjpbIlx1MDQyMFx1MDQzMFx1MDQzN1x1MDQ0MFx1MDQzMFx1MDQzMVx1MDQzZVx1MDQ0Mlx1MDQ0N1x1MDQzOFx1MDQzYSJdLCJleHAiOjE5ODcxMzY3MzJ9.18FEpvUm56aAuQ3RvpFE-kz7NkycIIJLSn7o3Isot2E'
base = 'http://127.0.0.1:5000/api/v1'
# base = 'http://unitapi.malevin.com/api/v1'

headers = {
    'Token': token,
    # 'Content-Type': 'application/json'
}

df = print_post(
    base + '/clc/production/actions/format_estimation_json_for_print',
    headers=headers,
    json={
        'est_id': 10
    }
)

pprint(df.json())



