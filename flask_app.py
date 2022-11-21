from flask import Flask, jsonify, make_response, request
from flask_restful import Resource, Api, reqparse, abort
from loguru import logger
from functools import wraps
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session, load_only
import json
from sqlalchemy import create_engine, inspect
from flask_restful import reqparse
from sqlalchemy.ext.automap import automap_base
from datetime import date, datetime
from flask.json import JSONEncoder
from api_modules import build_tables_fields_argparsers, create_db_resources_v2, CustomJSONEncoder, build_spec_argparsers, build_actions_argparsers
# from sqlalchemy.orm imposrt declarative_base


KEY = '89a10379-1373-4a2e-b331-0adc36157443'
creds = {
    'production': {
        "hostname": "194.67.116.213",
        "port": "3306",
        "username": "root",
        "password": "zs$N7b*7F2Zq",
        "dbname": "unit_clc"
    },
    'development': {
        "hostname": "194.67.116.213",
        "port": "3306",
        "username": "root",
        "password": "zs$N7b*7F2Zq",
        "dbname": "dev_CLC"
    }
}
engine, tables = create_db_resources_v2(creds)
tables_fields_argparsers = build_tables_fields_argparsers(engine['production'], tables, creds['production']['dbname'])
spec_tables_argparsers = build_spec_argparsers()
actions_argparsers = build_actions_argparsers()

def check_header(function=None):
    @wraps(function)
    def wrapper(*args, **kwargs):
        h = dict(request.headers)
        # logger.debug(h)
        if 'Key' not in h or h['Key'] != KEY:
            abort(401, message='Unauthorized')
        if 'Stage' not in h or h['Stage'] not in ['development', 'production']:
            abort(400, message="Specify stage of the project: development (for tests) or production. Note that if you work with special database for development tables' properties are still from real database. Watch both to have equal schemas for proper testing. Only data may differ.")
        kwargs['stage'] = h['Stage']
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
        for table_name, table_params in data['tables_to_glue'].items():
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
        return json_data


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


class Actions(Resource):
    @check_header
    def post(self, action_name, stage):
        if action_name not in actions_argparsers:
            abort(404, message='Action not found')
        session = Session(engine[stage])
        parser = actions_argparsers[action_name]
        args = parser.parse_args(strict=True)
        logger.debug(args)
        if action_name == 'give_clc_id_to_ek':
            update_eks_clc_id(session, args)
        


# def make_ek_details(session, stage, ek_id):
#     ek = tables['ek']
#     columns = ek.columns.keys()
#     ek = session.query(ek).filter(ek.c['id']==ek_id).one()
#     ek = {c: v for c, v in zip(columns, ek)}
#     return ek


def make_ek_materials_table(session, stage, ek_id):
    ek = tables['ek']
    columns = ek.columns.keys()
    ek = session.query(ek).filter(ek.c['id']==ek_id).one()
    ek = {c: v for c, v in zip(columns, ek)}
    # logger.debug(ek)

    est = tables['estimations']
    columns = est.columns.keys()
    est = session.query(est).filter(est.c['id']==ek['estimation_id']).one()
    est = {c: v for c, v in zip(columns, est)}
    # logger.debug(f'est: {est}')

    r_work_types_basic_materials = tables['r_work_types_basic_materials']
    r_work_types_basic_materials = session.query(r_work_types_basic_materials).with_entities(
        r_work_types_basic_materials.c['materials_id'], r_work_types_basic_materials.c['consumption_rate']
    ).filter(
        r_work_types_basic_materials.c['work_types_id']==ek['work_types_id'])
    r_work_types_basic_materials = pd.read_sql(r_work_types_basic_materials.statement, engine[stage])
    # logger.debug(r_work_types_basic_materials)

    r_ek_basic_materials = tables['r_ek_basic_materials']
    r_ek_basic_materials = session.query(r_ek_basic_materials).filter(r_ek_basic_materials.c['ek_id']==ek_id)
    r_ek_basic_materials = pd.read_sql(r_ek_basic_materials.statement, engine[stage])
    r_ek_basic_materials = r_ek_basic_materials.merge(r_work_types_basic_materials, how='left', on='materials_id')
    r_ek_basic_materials['volume'] = r_ek_basic_materials['consumption_rate'] * ek['volume']
    r_ek_basic_materials['is_basic'] = True
    
    r_ek_add_materials = tables['r_ek_add_materials']
    r_ek_add_materials = session.query(r_ek_add_materials).filter(r_ek_add_materials.c['ek_id']==ek_id)
    r_ek_add_materials = pd.read_sql(r_ek_add_materials.statement, engine[stage])
    r_ek_add_materials['is_basic'] = False

    df = pd.concat([r_ek_basic_materials, r_ek_add_materials], axis=0)

    mats = df.id.unique().tolist()
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

    return df


# def debug_func():
#     stage = 'production'
#     session = Session(engine[stage])
#     df = make_ek_table(session, stage, 2)


class SpecialTable(Resource):
    @check_header
    def get(self, table_name, stage):
        if table_name not in spec_tables_argparsers:
            abort(404, message='Special table not found')
        parser = spec_tables_argparsers[table_name]
        args = parser.parse_args(strict=True)
        # logger.debug(args)
        session = Session(engine[stage])
        if table_name == 'ek_mats':
            df = make_ek_materials_table(session, stage, **args)
        json_data = df.to_json(force_ascii=False, orient='records', date_format='iso')
        return json_data
        

app = Flask(__name__)
app.json_provider_class = CustomJSONEncoder
api = Api(app)
api.add_resource(Table, '/clc/api/v1/<table_name>')
api.add_resource(TableExpanded, '/clc/api/v1/expanded/<table_name>')
api.add_resource(SpecialTable, '/clc/api/v1/special/<table_name>')
api.add_resource(Actions, '/clc/api/v1/actions/<action_name>')
# Если таблицы нет, то выдает ошибку 500, нужно 404


if __name__ == '__main__':
    app.run(debug=False)
    # debug_func()
    # df = build_expanded_table_v2()
    # build_expanded_table('ep', 'production')





