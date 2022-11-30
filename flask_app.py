from flask import Flask, jsonify, make_response, request
from flask_restful import Resource, Api, reqparse, abort
from loguru import logger
import jwt
from functools import wraps
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session, load_only
from sqlalchemy.sql import text
import json
from sqlalchemy import create_engine, inspect
from flask_restful import reqparse
from sqlalchemy.ext.automap import automap_base
from datetime import date, datetime
from flask.json import JSONEncoder
from bcrypt import checkpw
from datetime import datetime, timedelta, timezone
from api_modules import build_tables_fields_argparsers, create_db_resources_v2, CustomJSONEncoder, build_spec_argparsers, build_actions_argparsers
# from sqlalchemy.orm imposrt declarative_base


KEY = '89a10379-1373-4a2e-b331-0adc36157443'
creds = {
    'production': {
        "hostname": "194.67.116.213",
        "port": "3306",
        "username": "root",
        "password": "zs$N7b*7F2Zq",
        "dbname": "unit_clc_main"
    },
    'development': {
        "hostname": "194.67.116.213",
        "port": "3306",
        "username": "root",
        "password": "zs$N7b*7F2Zq",
        "dbname": "dev_CLC"
    }
}
auth_creds = {
    "hostname": "194.67.116.213",
    "port": "3306",
    "username": "root",
    "password": "zs$N7b*7F2Zq",
    "dbname": "auth_db"
}
engine, tables, auth_engine, auth_tables = create_db_resources_v2(creds, auth_creds)

tables_fields_argparsers = build_tables_fields_argparsers(engine['production'], tables, creds['production']['dbname'])
spec_tables_argparsers = build_spec_argparsers()
actions_argparsers = build_actions_argparsers()

def check_header(function=None):
    @wraps(function)
    def wrapper(*args, **kwargs):
        h = dict(request.headers)
        if 'Key' not in h or h['Key'] != KEY:
            abort(401, message='Unauthorized')
        if 'Stage' not in h or h['Stage'] not in ['development', 'production']:
            abort(400, message="Specify stage of the project: development (for tests) or production. Note that if you work with special database for development tables' properties are still from real database. Watch both to have equal schemas for proper testing. Only data may differ.")
        kwargs['stage'] = h['Stage']
        res = function(*args, **kwargs)
        return res
    return wrapper


def check_developers_token(function=None):
    @wraps(function)
    def wrapper(*args, **kwargs):
        h = dict(request.headers)
        if 'Token' not in h:
            abort(400, message='Missing token in headers')
        try:
            res = jwt.decode(h['Token'], KEY, algorithms="HS256")
            logger.debug(res)
            # jwt.decode(token, KEY, algorithms="HS256", options={"verify_exp": False})
        except Exception as error:
            abort(401, message=f'Error: {str(error)}')
        if "Разработчик" not in res['roles']:
                abort(401, message='Request is allowed only for developers')
        res = function(*args, **kwargs)
        return res
    return wrapper


def check_token(function=None):
    @wraps(function)
    def wrapper(*args, **kwargs):
        h = dict(request.headers)
        if 'Token' not in h:
            abort(400, message='Missing token in headers')
        try:
            jwt.decode(h['Token'], KEY, algorithms="HS256")
            # return jsonify({"message": "Token is valid"})
        except Exception as error:
            abort(401, message=str(error))
        res = function(*args, **kwargs)
        return res
    return wrapper


def check_for_empty_table(q, multiple_records_abort=False):
    c = q.count()
    if c == 0:
        abort(404, message='Record is not found')
    if c > 1 and multiple_records_abort:
        abort(400, message='Multiple records found. Ask developers to check indexes in database and required parameters in API')


class Table(Resource):
    @check_header
    def post(self, table_name, stage):
        parser = tables_fields_argparsers[table_name]['upd']
        args = parser.parse_args(strict=True)
        session = Session(engine[stage])
        table = tables[table_name]
        primary_keys = [a.name for a in parser.args if a.required]
        values = {key: value for (key, value) in args.items() if key not in primary_keys}
        where_clauses = [table.c[key]==value for (key, value) in args.items() if key in primary_keys]
        q = session.query(table).filter(*where_clauses)
        check_for_empty_table(q, multiple_records_abort=True)
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

    @check_header
    def get(self, table_name, stage):
        parser = tables_fields_argparsers[table_name]['get']
        args = parser.parse_args(strict=True)
        session = Session(engine[stage])
        table = tables[table_name]
        where_clauses = [table.c[key]==value for (key, value) in args.items()]
        result = session.query(table).filter(*where_clauses)
        # check_for_empty_table(result)
        # Еще один способ отфильтровать, может пригодится
        # users.update().where(and_(*where_clauses)).values(**update[1])
        columns = table.columns.keys()
        d = [{c: v for c, v in zip(columns, row)} for row in result]
        return jsonify({"data": d})

    @check_header
    def put(self, table_name, stage):
        parser = tables_fields_argparsers[table_name]['put']
        args = parser.parse_args(strict=True)
        session = Session(engine[stage])
        table = tables[table_name]
        # Вставлять список не рационально, если передается всего 1 строка, переделать!
        insert_list = [args]
        try:
            session.execute(table.insert(), insert_list)
            session.commit()
            return '', 201
        except Exception as error:
            session.rollback()
            response = make_response(jsonify(
                {'error': str(error)}
            ), 403)
            # response.headers["Content-Type"] = "application/json"
            return response

    @check_header
    def delete(self, table_name, stage):
        parser = tables_fields_argparsers[table_name]['del']
        args = parser.parse_args(strict=True)
        session = Session(engine[stage])
        table = tables[table_name]
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
            # response.headers["Content-Type"] = "application/json"
            return response


# def get_dfs(params, base_table_name, stage='production'):
#     dfs = []
#     session = Session(engine[stage])
#     for table_name, columns in params.items():
#         table = tables[table_name]
#         query = session.query(table)
#         if len(columns) > 0:
#             fields = [table.c[col] for col in columns]
#             query = query.with_entities(*fields)
#         # query = query.filter()
#         # logger.debug(columns)
#         df = pd.read_sql(query.statement, engine[stage])
#         if table_name != base_table_name:
#             df = df.add_prefix(table_name+'_')
#         logger.debug(df)
#         dfs.append(df)
#     return dfs


# def glue_tables(session, base_df, tables_to_glue, stage):
#     for table_name, table_params in tables_to_glue.items():
#         table = tables[table_name]
#         query = session.query(table)
#         if 'remain_cols' in table_params and len(table_params['remain_cols']) > 0:
#             fields = [table.c[col] for col in table_params['remain_cols']]
#             query = query.with_entities(*fields)
#             table_params.pop('remain_cols', None)
#         df = pd.read_sql(query.statement, engine[stage])
#         table_params['right_on'] = table_name + '_' + table_params['right_on']
#         base_df = base_df.merge(df.add_prefix(table_name+'_'), how='left', **table_params)
#     return base_df


class TableExpanded(Resource):
    @check_header
    def post(self, table_name, stage):
        data = request.json
        session = Session(engine[stage])
        base_table = tables[table_name]
        base_query = session.query(base_table)
        base_df = pd.read_sql(base_query.statement, engine[stage])
        logger.debug(base_df)
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
            df = pd.read_sql(query.statement, engine[stage])
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
        return response


def update_eks_clc_id(session, args):
    ek = tables['ek']
    try:
        session.query(ek).filter(ek.c['id'].in_(args['ek_ids'])).update({'clc_id': args['clc_id']})
        session.commit()
        return '', 204
    except Exception as error:
        session.rollback()
        response = make_response(jsonify(
            {'error': str(error)}
        ), 403)
        return response


def update_mats_spc_id(session, args):
    r_ek_basic_materials = tables['r_ek_basic_materials']
    try:
        session.query(r_ek_basic_materials).filter(r_ek_basic_materials.c['id'].in_(args['r_ek_basic_mats_ids'])).update({'spc_id': args['spc_id']})
        session.commit()
        return '', 204
    except Exception as error:
        session.rollback()
        response = make_response(jsonify(
            {'error': str(error)}
        ), 403)
        return response


class Actions(Resource):
    @check_header
    def post(self, action_name, stage):
        if action_name not in actions_argparsers:
            abort(404, message='Action not found')
        session = Session(engine[stage])
        parser = actions_argparsers[action_name]
        args = parser.parse_args(strict=True)
        # logger.debug(args)
        if action_name == 'give_clc_id_to_ek':
            update_eks_clc_id(session, args)
        if action_name == 'give_spc_id_to_material':
            update_mats_spc_id(session, args)


# def make_ek_details(session, stage, ek_id):
#     ek = tables['ek']
#     columns = ek.columns.keys()
#     ek = session.query(ek).filter(ek.c['id']==ek_id).one()
#     ek = {c: v for c, v in zip(columns, ek)}
#     return ek


def make_est_materials_table(session, stage, est_id):
    est = tables['estimations']
    columns = est.columns.keys()
    est = session.query(est).filter(est.c['id']==est_id).one()
    est = {c: v for c, v in zip(columns, est)}

    ek = tables['ek']
    ek = session.query(ek).filter(
        ek.c['estimation_id']==est_id)
    ek = pd.read_sql(ek.statement, engine[stage])
    
    # logger.debug(f'est: {est}')

    r_work_types_basic_materials = tables['r_work_types_basic_materials']
    r_work_types_basic_materials = session.query(r_work_types_basic_materials).filter(
        r_work_types_basic_materials.c['work_types_id'].in_(ek['work_types_id']))
    r_work_types_basic_materials = pd.read_sql(r_work_types_basic_materials.statement, engine[stage])
    # logger.debug(r_work_types_basic_materials)

    r_ek_basic_materials = tables['r_ek_basic_materials']
    r_ek_basic_materials = session.query(r_ek_basic_materials).filter(r_ek_basic_materials.c['ek_id'].in_(ek.id))
    r_ek_basic_materials = pd.read_sql(r_ek_basic_materials.statement, engine[stage])
    # r_ek_basic_materials = r_ek_basic_materials.merge(r_work_types_basic_materials, how='left', on='materials_id')
    # r_ek_basic_materials['volume'] = r_ek_basic_materials['consumption_rate'] * ek['volume']
    r_ek_basic_materials['is_basic'] = True
    
    r_ek_add_materials = tables['r_ek_add_materials']
    r_ek_add_materials = session.query(r_ek_add_materials).filter(r_ek_add_materials.c['ek_id'].in_(ek.id))
    r_ek_add_materials = pd.read_sql(r_ek_add_materials.statement, engine[stage])
    r_ek_add_materials['is_basic'] = False

    df = pd.concat([r_ek_basic_materials, r_ek_add_materials], axis=0)

    logger.debug(df)
    # raise Exception
    materials = tables['materials']
    materials = session.query(materials).filter(
        materials.c['id'].in_(df['materials_id']))
    materials = pd.read_sql(materials.statement, engine[stage])
    logger.debug(materials)

    # logger.debug(r_work_types_basic_materials)
    df = df.merge(materials, how='left', left_on='materials_id', right_on='id', suffixes=[None, '_mat'])
    df = df.merge(ek, how='left', left_on='ek_id', right_on='id', suffixes=[None, '_ek'])
    logger.debug(df.columns)
    df = df.merge(r_work_types_basic_materials, how='left', left_on=['work_types_id', 'materials_id'], right_on=['work_types_id', 'materials_id'])
    # raise Exception
    logger.debug(df)
    df['volume'].loc[df['is_basic']] = df['consumption_rate'] * df['volume_ek']
    logger.debug(df.volume)

    mats = df.materials_id.unique().tolist()
    prices_history = tables['materials_prices_history']
    prices_history = session.query(prices_history).filter(
        prices_history.c['materials_id'].in_(mats),
        prices_history.c['objects_id']==est['objects_id']
    )

    prices_history = pd.read_sql(prices_history.statement, engine[stage])
    # Логику цен переделать!
    prices_history = prices_history[['materials_id', 'price']]
    prices_history.drop_duplicates(subset=['materials_id'], keep='last', inplace=True)
    df = df.merge(prices_history, how='left', left_on='id', right_on='materials_id', suffixes=[None, '_mph'])
    df['true_price'] = df['closed_price'].fillna(df['price'])
    df.drop(columns=['materials_id', 'price', 'materials_id_mph'], inplace=True)
    logger.debug(df)
    logger.debug(df.columns)

    return df


def debug_func():
    stage = 'production'
    session = Session(engine[stage])
    df = make_est_materials_table(session, stage, 1)


class SpecialTable(Resource):
    @check_header
    def get(self, table_name, stage):
        if table_name not in spec_tables_argparsers:
            abort(404, message='Special table not found')
        parser = spec_tables_argparsers[table_name]
        args = parser.parse_args(strict=True)
        session = Session(engine[stage])
        if table_name == 'est_mats':
            df = make_est_materials_table(session, stage, **args)
        json_data = df.to_json(force_ascii=False, orient='records', date_format='iso')
        response = make_response(json_data, 200)
        response.headers["Content-Type"] = "application/json"
        return response


class Auth(Resource):
    @check_token
    def post(self):
        return jsonify({"message": "Token is valid"})

    def get(self):
        session = Session(auth_engine)
        parser = actions_argparsers['auth']
        args = parser.parse_args(strict=True)
        # logger.debug(args)
        table = auth_tables['users']
        query = session.query(table).filter(table.c['email']==args['email'])
        if query.count() == 0:
            abort(401, message='Invalid email or password')
        columns = table.columns.keys()
        user = {c: v for c, v in zip(columns, query[0])}
        logger.debug(user)
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
        logger.debug(user_roles)
        payload_data = {
            "name": user["name"],
            "roles": user_roles, 
            "exp": datetime.now(timezone.utc) + timedelta(hours=16)
        }
        token = jwt.encode(payload_data, KEY)
        return token


class SQL_execute(Resource):
    @check_developers_token
    def post(self, database):
        parser = actions_argparsers['sql']
        args = parser.parse_args(strict=True)
        qs = args['query']
        qs = [qs] if type(qs) == str else qs
        h = dict(request.headers)
        if database in ['clc']:
            if 'Stage' not in h or h['Stage'] not in ['development', 'production']:
                abort(400, message="Specify stage of the project: development (for tests) or production. Note that if you work with special database for development tables' properties are still from real database. Watch both to have equal schemas for proper testing. Only data may differ.")
        elif database == 'auth':
            if 'Stage' in h:
                abort(400, message="Database 'auth' does not have a copy for development. Do not specify stage.")
        if database == 'auth':
            eng = auth_engine
        elif database == 'clc':
            eng = engine[h['Stage']]
        else:
            abort(400, message='Unknown database')
        ans = []
        with eng.connect() as con:
            for q in qs:
                is_allowed = q.lower().startswith(('select', 'update', 'insert', 'delete'))
                if not is_allowed:
                    ans.append({'query': q, 'success': False, 'error': 'SELECT, UPDATE, INSERT and DELETE queries are not allowed'})
                    continue
                try:
                    rs = con.execute(text(q))
                    ans.append({'query': q, 'success': True, 'error': None})
                except Exception as error:
                    logger.error('Ошибка при выполнении запроса')
                    ans.append({'query': q, 'success': False, 'error': str(error)})
        return jsonify(ans)


app = Flask(__name__)
app.json_provider_class = CustomJSONEncoder
api = Api(app)
api.add_resource(Table, '/clc/api/v1/<table_name>')
api.add_resource(TableExpanded, '/clc/api/v1/expanded/<table_name>')
api.add_resource(SpecialTable, '/clc/api/v1/special/<table_name>')
api.add_resource(Actions, '/clc/api/v1/actions/<action_name>')
api.add_resource(Auth, '/auth')
api.add_resource(SQL_execute, '/execute_sql/<database>')
# Если таблицы нет, то выдает ошибку 500, нужно 404


if __name__ == '__main__':
    app.run(debug=False)







