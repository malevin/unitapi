from flask import Flask, jsonify, make_response, request
from flask_restful import Resource, Api, reqparse, abort
from loguru import logger
from functools import wraps
import pandas as pd
from sqlalchemy.orm import Session, load_only
import json
from sqlalchemy import create_engine, inspect
from flask_restful import reqparse
from sqlalchemy.ext.automap import automap_base
from datetime import date, datetime
from flask.json import JSONEncoder
from api_modules import build_tables_fields_argparsers, create_db_resources_v2, CustomJSONEncoder
# from sqlalchemy.orm imposrt declarative_base


KEY = '89a10379-1373-4a2e-b331-0adc36157443'
creds = {
    'production': {
        "hostname": "194.67.116.213",
        "port": "3306",
        "username": "root",
        "password": "zs$N7b*7F2Zq",
        "dbname": "scandia_clc"
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


def check_header(function=None):
    @wraps(function)
    def wrapper(*args, **kwargs):
        h = dict(request.headers)
        logger.debug(h)
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


# def build_expanded_table(table_name, stage):
#     # session = Session(engine[stage])
#     # table = tables[table_name]
#     params = {
#         'ep': [],
#         'objects': ['id', 'name'],
#         'ep_types': []
#     }
#     ep, objects, ep_types = get_dfs(params, 'ep', 'production')
#     params2 = [
#         {
#             'df': objects,
#             'left_on': 'objects_id',
#             'right_on': 'objects_id'
#         },
#         {
#             'df': ep_types,
#             'left_on': 'ep_types_id',
#             'right_on': 'ep_types_id'
#         },
#     ]
#     ep_expanded = glue_dfs_by_key(ep, params2)
#     logger.debug(ep_expanded)
    
    # where_clauses = [table.c[key]==value for (key, value) in args.items()]
    # query = session.query(table).filter()

    # # Convert to DataFrame
    # ep = pd.read_sql(query.statement, engine[stage])
    
    # fields = ['id', 'name']
    # table = tables['objects']
    # fields_sql = [table.c[key] for c in fields]
    # query = session.query(table).with_entities(*fields)

    # objects = pd.read_sql(query.statement, engine[stage])
    # ep = ep.merge(objects, how='left', left_on='objects_id', right_on='id')
    # logger.debug(ep)


# def get_tables_to_glue_params(table_name):
#     if table_name == 'ep':
#         tables_to_glue = {
#             'objects': {
#                 'remain_cols': ['id', 'name'],
#                 'left_on': 'objects_id',
#                 'right_on': 'id',
#             },
#             'ep_types': {
#                 'left_on': 'ep_types_id',
#                 'right_on': 'id'
#             },
#             # 'work_types': {
#             #     'left_on': 'ep_types_id', # колонка слева с названием до добавления суффикса
#             #     'right_on': 'ep_types_id' # колонка справа после добавления суффикса
#             #     # Поэтому если склейка идет уже не с первичной таблицей, нужно отдельно задавать
#             #     # левую и правую колонки, по которым идет склеивание
#             # }
#         }
    # if table_name == 'ek':
    #     tables_to_glue = {
    #         'objects': {
    #             'remain_cols': ['id', 'name'],
    #             'left_on': 'objects_id',
    #             'right_on': 'id',
    #         },
    #         'ep_types': {
    #             'left_on': 'ep_types_id',
    #             'right_on': 'id'
    #         },
    #     }
#     return tables_to_glue

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


class Table_expanded(Resource):
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
        base_df = base_df.to_dict(orient='records')
        # json_data = base_df.to_json(force_ascii=False, orient='records', date_format='iso')
        return jsonify({"data": base_df})


app = Flask(__name__)
app.json_provider_class = CustomJSONEncoder
api = Api(app)
api.add_resource(Table, '/clc/api/v1/<table_name>')
api.add_resource(Table_expanded, '/clc/api/v1/expanded/<table_name>')
# Если таблицы нет, то выдает ошибку 500, нужно 404


if __name__ == '__main__':
    app.run(debug=True)
    # df = build_expanded_table_v2()
    # build_expanded_table('ep', 'production')







