# '''
# Это основной исполняемый файл сервера. Конфигурация WSGI создается автоматически, в репозитории нет.
# Хостится на PythonAnywhere. Код загружаю в GitHUB, там на push стоит триггер для обновления
# кода на сервере.

# API разработан для строительной компании Scandia в Новосибирске.
# В компании работают несколько продуктов. Основые - калькулятор для расчета стоимости
# строительства и управленческий учет для ведения договоров, актов, бухгалтерского учета и т.д.
# API единый на все продукты.
# '''

from flask import Flask, jsonify, make_response, request
from flask_restful import Resource, Api, abort
from loguru import logger
import jwt
from functools import wraps
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy.sql import text
from sqlalchemy import or_, and_
from datetime import datetime, timedelta, timezone
from bcrypt import checkpw
from flask_json import FlaskJSON
from pprint import pprint
from uu_actions import *
from clc_actions import *
from api_modules import build_init_tables_argparsers, create_db_resources_v3,build_spec_argparsers, build_actions_argparsers
from api_modules import get_df_from_db, get_table_from_db
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None


# Ключ для кодирования JWT токенов
KEY = '89a10379-1373-4a2e-b331-0adc36157443'


creds = {
    # БД для авторизации во все продукты, исключение из иерархии, описанной выше
    'auth': {
        'production': {
            "hostname": "194.67.116.213",
            "port": "3306",
            "username": "root",
            "password": "zs$N7b*7F2Zq",
            "dbname": "auth_db"
        }
    },
    # калькулятор
    'clc': {
        # рабочая база
        'production': {
            "hostname": "194.67.116.213",
            "port": "3306",
            "username": "root",
            "password": "zs$N7b*7F2Zq",
            "dbname": "unit_clc_main"
        },
        # тестовая база
        'development': {
            "hostname": "194.67.116.213",
            "port": "3306",
            "username": "root",
            "password": "zs$N7b*7F2Zq",
            "dbname": "dev_CLC"
        }
    },
    # управленческий учет
    'uu': {
        # СЗ Скандиа
        'scandia': {
            "hostname": "194.67.116.213",
            "port": "3306",
            "username": "root",
            "password": "zs$N7b*7F2Zq",
            "dbname": "scandia_UU"
        },
        # СЗ ЭсПиВи
        'spv': {
            "hostname": "194.67.116.213",
            "port": "3306",
            "username": "root",
            "password": "zs$N7b*7F2Zq",
            "dbname": "spv_UU"
        },
        # СЗ Юнит
        'unit': {
            "hostname": "194.67.116.213",
            "port": "3306",
            "username": "root",
            "password": "zs$N7b*7F2Zq",
            "dbname": "unit_UU"
        },
        # ООО ЮнитГрад
        'unitgrad': {
            "hostname": "194.67.116.213",
            "port": "3306",
            "username": "root",
            "password": "zs$N7b*7F2Zq",
            "dbname": "unitgrad_UU"
        },
        # СЗ ЭмКаДэ
        'mkd': {
            "hostname": "194.67.116.213",
            "port": "3306",
            "username": "root",
            "password": "zs$N7b*7F2Zq",
            "dbname": "mkd_UU"
        },
        # тестовая
        'development': {
            "hostname": "194.67.116.213",
            "port": "3306",
            "username": "root",
            "password": "zs$N7b*7F2Zq",
            "dbname": "dev_UU"
        },
    }
}

engines, db_tables, inspectors = create_db_resources_v3(creds)
parsers = {}
parsers['initial'] = build_init_tables_argparsers(engines, db_tables, creds), # Для работы с таблицами
parsers['actions'] = build_actions_argparsers(creds), # Специальные действия в продуктах
parsers['special'] = build_spec_argparsers(creds) # Сложные склейки таблиц


# В токенах, которые запрашивает API содержится пэйлоад с должностью, именем и сроком действия.
# Этот декоратор проверяет, что токен был выдан разработчику. Некоторые маршруты доступны только им.
def check_developers_token(function=None):
    @wraps(function)
    def wrapper(*args, **kwargs):
        h = dict(request.headers)
        if 'Token' not in h:
            abort(400, message='Missing token in headers')
        try:
            res = jwt.decode(h['Token'], KEY, algorithms="HS256")
        except Exception as error:
            abort(401, message=f'Error: {str(error)}')
        if "Разработчик" not in res['roles']:
                abort(401, message='Request is allowed only for developers')
        res = function(*args, **kwargs)
        return res
    return wrapper


# Декоратор, проверяющий подпись и срок действия токена
def check_token(function=None):
    @wraps(function)
    def wrapper(*args, **kwargs):
        h = dict(request.headers)
        if 'Token' not in h:
            abort(400, message='Missing token in headers')
        try:
            jwt.decode(h['Token'], KEY, algorithms="HS256")
        except Exception as error:
            abort(401, message=str(error))
        res = function(*args, **kwargs)
        return res
    return wrapper


# Декоратор, который достает нужный парсер в зависимости от маршрута,
# по которому пришел запрос, и от вызванного метода.
# Возвращает объект сессии, объект со всеми таблицами БД и распарсенные параметры запроса
def get_init_table_args(function=None):
    @wraps(function)
    def wrapper(*args, **kwargs):
        product, db, resource = kwargs['product'], kwargs['db'], kwargs['table_name']
        mthd = request.method
        eng = engines[product][db]
        session = Session(eng)
        table = db_tables[product][db][resource]
        psr = parsers['initial'][product][db][resource][mthd]
        prs_args = psr.parse_args(strict=True)
        kwargs = {'session': session, 'table': table, 'args': prs_args}
        if mthd == 'PUT':
            kwargs['primary_keys'] = [a.name for a in psr.args if a.required]
        res = function(*args, **kwargs)
        return res
    return wrapper


# Ресурс для получения таблиц, редактирования, добавления и удаления строчек
class Table(Resource):
    @check_token
    @get_init_table_args
    def put(self, session, table, args, primary_keys):
        values = {key: value for (key, value) in args.items() if key not in primary_keys}
        where_clauses = [table.c[key]==value for (key, value) in args.items() if key in primary_keys]
        q = session.query(table).filter(*where_clauses)
        # Проверка единственной найденной строки
        check_for_empty_table(q, multiple_records_abort=True)
        # Откат БД к прежнему состоянию в случае ошибки. 
        # Такая конструкция используется много где, поэтому в ближайшее время должна быть заменена декоратором.
        # Над текстом и кодом ошибки не заморачиваюсь, возвращаю то, что пишет мне SQL. Пользуюсь этим
        # преимущественно я, мне ошибки понятны.
        try:
            q.update(values)
            session.commit()
            return '', 204
        except Exception as error:
            session.rollback()
            response = make_response(jsonify(
                {'error': str(error)}
            ), 403)
            return response

    @check_token
    @get_init_table_args
    def get(self, session, table, args):
        where_clauses = [table.c[key]==value for (key, value) in args.items()]
        result = session.query(table).filter(*where_clauses)
        columns = table.columns.keys()
        d = [{c: v for c, v in zip(columns, row)} for row in result]
        return jsonify(d)

    @check_token
    @get_init_table_args
    def post(self, session, table, args):
        insert_list = [args] # Вставлять список не рационально, если передается всего 1 строка, переделать!
        try:
            session.execute(table.insert(args))
            session.commit()
            return '', 201
        except Exception as error:
            session.rollback()
            response = make_response(jsonify(
                {'error': str(error)}
            ), 403)
            return response

    @check_token
    @get_init_table_args
    def delete(self, session, table, args):
        where_clauses = [table.c[key]==value for (key, value) in args.items()]
        q = session.query(table).filter(*where_clauses)
        check_for_empty_table(q, multiple_records_abort=True)
        try:
            q.delete()
            session.commit()
            return '', 204
        except Exception as error:
            session.rollback()
            response = make_response(jsonify(
                {'error': str(error)}
            ), 403)
            return response


# Декоратор проверяет, содержит ли запрос POST на склейку таблиц json объект
def get_expanded_table_args_post(function=None):
    @wraps(function)
    def wrapper(*args, **kwargs):
        product, db = kwargs['product'], kwargs['db']
        eng = engines[product][db]
        session = Session(eng)
        tables = db_tables[product][db]
        if len(request.data) != 0:
            try:
                data = request.json
            except Exception as error:
                abort(400, message=str(error))
        else:
            abort(400, message='JSON object must be passed in data to join tables')
        kw = {'eng': eng, 'data': data, 'session': session, 'tables': tables, 'table_name': kwargs['table_name']}    
        res = function(*args, **kw)
        return res
    return wrapper


# Декоратор проверяет, есть ли запрашиваемое представление в базе.
# Если есть, то возвращает его определение, движок БД и опциональные аргументы для фильтрации
def get_expanded_table_args_get(function=None):
    @wraps(function)
    def wrapper(*args, **kwargs):
        product, db, table_name = kwargs['product'], kwargs['db'], kwargs['table_name']
        eng = engines[product][db]
        insp = inspectors[product][db]
        existing_views = insp.get_view_names()
        if table_name in existing_views:
            definition = insp.get_view_definition(table_name)
        else:
            abort(404, message=f"'{table_name}' not found in existing views")
        definition = definition[definition.find('select'):]
        psr_args = request.args
        kw = {'definition': definition, 'eng': eng, 'psr_args': psr_args}
        res = function(*args, **kw)
        return res
    return wrapper


# Ресурс позволяет получить расширенные таблицы. Метод GET возвращает существующее в БД представление.
# Метод POST склеивает таблицы произвольно, ожидая на вход название table_name первой таблицы , к которой будут
# присоединены все остальные, и json объект определенной структуры:
# {
#     "tables_to_glue": {
#         "<таблица 1>": {
#             "remain_cols": ["<столбец 1>", "<столбец 2>", "<столбец 3>"],
#             "left_on": "<поле в исходной таблице>",
#             "right_on": "<поле в присоединяемой таблице 1>"
#         },
#         "{таблица 2}": {
#             "remain_cols": ["<столбец 1>", "<столбец 2>", "<столбец 3>"],
#             "left_on": "<поле в исходной таблице либо поле из таблицы 1 с учетом его переименования после присоединения к исходной таблице в формате <имяТаблицы1_полеТаблицы1>",
#             "right_on": "<поле в присоединяемой таблице 2>"
#         },
#         "<таблица 3>": {
#             "left_on": "<имяТаблицы2_полеТаблицы2>",
#             "right_on": "<поле в присоединяемой таблице 3>"
#         },
#     },
#     "filter_by": {
#       "<полеИсходнойТаблицы>": <значение>,
#       "<имяТаблицы2_полеТаблицы2>": <значение>
#     }
# }
class TableExpanded(Resource):
    @check_token
    @get_expanded_table_args_get
    def get(self, definition, eng, psr_args):
        with eng.connect() as con:
            result = con.execute(text(definition))
            res = [dict(r) for r in result]
            if len(psr_args) != 0:
                res = [r for r in res for k, v in psr_args.items() if str(r[k]) == v]
            return jsonify(res)

    @check_token
    @get_expanded_table_args_post
    def post(self, eng, data, session, tables, table_name):
        try:
            base_table = tables[table_name]
            base_query = session.query(base_table)
            base_df = pd.read_sql(base_query.statement, eng)
            for table_name, table_params in data['tables_to_glue'].items():
                if base_df[table_params['left_on']].isnull().all():
                    if 'remain_cols' in table_params and len(table_params['remain_cols']) > 0:
                        cols = [table_name + '_' + c for c in table_params['remain_cols']]
                        base_df[cols] = None
                    continue
                table = tables[table_name]
                query = session.query(table)
                if 'remain_cols' in table_params and len(table_params['remain_cols']) > 0:
                    fields = [table.c[col] for col in table_params['remain_cols']]
                    query = query.with_entities(*fields)
                    table_params.pop('remain_cols', None)
                df = pd.read_sql(query.statement, eng)
                table_params['right_on'] = table_name + '_' + table_params['right_on']
                base_df = base_df.merge(df.add_prefix(table_name+'_'), how='left', **table_params)
            if 'filter_by' in data and len(data['filter_by']) > 0:
                base_df = base_df.loc[(base_df[list(data['filter_by'])] == pd.Series(data['filter_by'])).all(axis=1)]
            # Конвертация датафрейма в словарь, а затем преобразование в джейсон с помощью jsonify
            # не конвертирует в null тип данных NaN библиотеки numpy питона, поэтому используется конвертация pandas
            # base_df = base_df.to_dict(orient='records')
            # return jsonify({"data": base_df})
            json_data = base_df.to_json(force_ascii=False, orient='records', date_format='iso')
            response = make_response(json_data, 200)
            response.headers["Content-Type"] = "application/json"
        except Exception as error:
            logger.exception(error)
            abort(400, message=f'Error when joining tables. Check JSON object passed in request data. Error from server code: {str(error)}')
        return response


# Декоратор для получения аргументов, движка, сессии и т.д., чтобы методы содержали
# только смысловую часть изменений в БД и не забивались одинаковой рутиной
def get_actions_special_default_args(function=None):
    @wraps(function)
    def wrapper(*args, **kwargs):
        db, resource_name = kwargs['db'], kwargs['resource_name']
        endpt = str(request.url_rule).split('/')
        product, branch = endpt[-4], endpt[-2]
        eng = engines[product][db]
        session = Session(eng)
        tables = db_tables[product][db]
        try:
            # Ищем парсер специально для этой БД
            psr = parsers[branch][product][db][resource_name]
        except:
            try:
                # Ищем парсер для любой БД в этом продукте
                psr = parsers[branch][product]['COMMON'][resource_name]
            except:
                try:
                    # Ищем парсер для любой БД любого продукта
                    psr = parsers[branch]['COMMON'][resource_name]
                except:
                    abort(404, message='Action or special table not found')
        prs_args = psr.parse_args(strict=True)
        kw = {'eng': eng, 'session': session, 'tables': tables, 'args': prs_args, 'resource_name': resource_name}
        res = function(*args, **kw)
        return res
    return wrapper


# Специальные действия в Управленческом учете
# pack - реестр на оплату (содержит несколько заявок)
# payment_request - заявка на оплату
class UuActions(Resource):
    @check_token
    @get_actions_special_default_args
    def post(self, eng, session, tables, args, resource_name):
        if resource_name == 'approve_payment_requests':
            ans = approve_payment_requests(session, tables, args)
        elif resource_name == 'decline_payment_requests':
            ans = decline_payment_requests(session, tables, args)
        elif resource_name == 'set_payment_requests_into_pack':
            ans = set_payment_requests_into_pack(session, tables, args)
        elif resource_name == 'create_pack_with_payment_requests':
            ans = create_pack_with_payment_requests(session, tables, args)
        elif resource_name == 'delete_pack_with_payment_requests':
            ans = delete_pack_with_payment_requests(session, tables, args)
        return ans


# Специальные действия в Калькуляторе. Сокращения:
# clc - calculation - калькуляция
# ek - единичная калькуляция (одна работа с материалами внутри КЛК)
# mat - material - материал
# spc - specification - спецификация на материалы
# est - estimation - расчет всех работ и материалов по дому на статью
class CalculatorActions(Resource):
    @check_token
    @get_actions_special_default_args
    def post(self, eng, session, tables, args, resource_name):
        if resource_name == 'give_clc_id_to_ek':
            update_eks_clc_id(session, tables, args)
        elif resource_name == 'give_spc_id_to_material':
            update_mats_spc_id(session, tables, args)
        elif resource_name == 'delete_ek_with_mats':
            delete_ek_with_mats(session, tables, args['ek_ids'])
        elif resource_name == 'delete_clc_with_eks':
            delete_clc_with_eks(session, tables, args['clc_ids'])
        elif resource_name == 'delete_spc_with_mats':
            delete_spc_with_mats(session, tables, args['spc_ids'])
        elif resource_name == 'format_estimation_json':
            json_data = format_estimation_json(eng, session, tables, args['est_id'])
            response = make_response(jsonify(json_data), 200)
            response.headers["Content-Type"] = "application/json"
            return response


# Ресурс для сложных составных таблиц калькулятора
class CalculatorSpecialTables(Resource):
    @check_token
    @get_actions_special_default_args
    def get(self, eng, session, tables, args, resource_name):
        if resource_name == 'est_mats':
            df = make_est_materials_table(eng, session, tables, **args)
        # elif ...
        json_data = df.to_json(force_ascii=False, orient='records', date_format='iso')
        response = make_response(json_data, 200)
        response.headers["Content-Type"] = "application/json"
        return response


# Ресурс для авторизации
class Auth(Resource):
    @check_token
    # Проверка валидности токена
    def post(self):
        return jsonify({"message": "Token is valid"})

    # Получение токена. Токен дается один. Краткосрочный и долгосрочный смысла делать
    # не было, авторизация сделана для внутреннего пользования и различения сотрудников.
    # API используется внутри коструктора приложений Retool, где приобрели один аккаунт на
    # всех сотрудников. Никаких требований к токену касаемо защиты не стоит. В пэйлоаде
    # содержится ФИО и роли сотрудника
    def get(self):
        session = Session(engines['auth']['production'])
        parser = parsers['actions']['auth']['COMMON']['check_pwd']
        args = parser.parse_args(strict=True)
        auth_tables = db_tables['auth']['production']
        table = auth_tables['users']
        query = session.query(table).filter(table.c['email']==args['email'])
        if query.count() == 0:
            abort(401, message='Invalid email or password')
        columns = table.columns.keys()
        user = {c: v for c, v in zip(columns, query[0])}
        if not checkpw(args['password'].encode('utf8'), user['password'].encode('utf-8')):
            abort(401, message='Invalid email or password')
        table = auth_tables['r_users_roles']
        columns = table.columns.keys()
        result = session.query(table).filter(table.c['user_id'] == user['id'])
        user_roles_ids = [v for row in result for c, v in zip(columns, row) if c == 'role_id']
        table = auth_tables['roles']
        columns = table.columns.keys()
        result = session.query(table).filter(table.c['id'].in_(user_roles_ids))
        user_roles = [v for row in result for c, v in zip(columns, row) if c == 'name']
        payload_data = {
            "name": user["name"],
            "roles": user_roles, 
            "exp": datetime.now(timezone.utc) + timedelta(hours=500)
        }
        token = jwt.encode(payload_data, KEY)
        return token, 200


# Маршрут для выполнения SQL запросов к БД. Судя по всему в MySQL Workbench существует проблема.
# При запросах на добавление или удаление полей таблица ломается и перестает отвечать, при этом
# все остальные таблицы работают нормально. Таблица при этом восстанавливается в течение суток и все изменения
# сохраняются. Проблема не только у меня. Руки не доходят выяснить, в чем дело, поэтому написан маршрут.
# Доступен только по токену разработчика
class SQL_execute(Resource):
    @check_developers_token
    def post(self, product, db):
        psr = parsers['actions']['COMMON']['sql']
        args = psr.parse_args(strict=True)
        qs = args['query']
        qs = [qs] if type(qs) == str else qs
        try:
            eng = engines[product][db]
        except:
            abort(400, message='Unknown project and or its database')
        ans = []
        with eng.connect() as con:
            for q in qs:
                not_allowed = q.lower().startswith(('select', 'update', 'insert', 'delete'))
                if not_allowed:
                    ans.append({'query': q, 'success': False, 'error': 'SELECT, UPDATE, INSERT and DELETE queries are not allowed'})
                    continue
                try:
                    rs = con.execute(text(q))
                    ans.append({'query': q, 'success': True, 'error': None})
                except Exception as error:
                    logger.error('Ошибка при выполнении запроса')
                    ans.append({'query': q, 'success': False, 'error': str(error)})
        return jsonify(ans)


# Временная функция для тестирования json для распечатки расчета
# def debug():
#     tables = db_tables['clc']['production']
#     eng = engines['clc']['production']
#     session = Session(eng)
#     format_estimation_json(eng, session, tables, 10, True)


app = Flask(__name__)
json = FlaskJSON(app)
# app.config['JSON_DATETIME_FORMAT'] = '%Y/%m/%d %H:%M:%S'
api = Api(app)
api.add_resource(Table, '/api/v1/<product>/<db>/initial/<table_name>')
api.add_resource(TableExpanded, '/api/v1/<product>/<db>/expanded/<table_name>')
api.add_resource(CalculatorSpecialTables, '/api/v1/clc/<db>/special/<resource_name>')
api.add_resource(CalculatorActions, '/api/v1/clc/<db>/actions/<resource_name>')
api.add_resource(UuActions, '/api/v1/uu/<db>/actions/<resource_name>')
api.add_resource(Auth, '/api/v1/auth')
api.add_resource(SQL_execute, '/api/v1/<product>/<db>/execute_sql')


if __name__ == '__main__':
    app.run(debug=True)
    # debug()







