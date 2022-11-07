from flask import Flask, jsonify, make_response, request #, Response, jsonify, 
from flask_restful import Resource, Api, reqparse, abort #, fields, marshal_with
from loguru import logger
from functools import wraps
# import pandas as pd
from sqlalchemy.orm import Session
import json
from sqlalchemy import create_engine, inspect
from flask_restful import reqparse
from sqlalchemy.ext.automap import automap_base
from datetime import date, datetime
from flask.json import JSONEncoder
from api_modules import build_tables_fields_argparsers, create_db_resources, CustomJSONEncoder
# from sqlalchemy.orm import declarative_base


KEY = '89a10379-1373-4a2e-b331-0adc36157443'
creds = {
    "hostname": "194.67.116.213",
    "port": "3306",
    "username": "root",
    "password": "zs$N7b*7F2Zq",
    "dbname": "scandia_clc"
}
engine, tables = create_db_resources(creds)
tables_fields_argparsers = build_tables_fields_argparsers(engine, tables, creds['dbname'])


def check_api_key_v2(function=None):
    @wraps(function)
    def wrapper(*args, **kwargs):
        h = dict(request.headers)
        if 'Key' not in h or h['Key'] != KEY:
            abort(401, message='Unauthorized')
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
    @check_api_key_v2
    def post(self, table_name):
        parser = tables_fields_argparsers[table_name]['upd']
        args = parser.parse_args(strict=True)
        session = Session(engine)
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

    @check_api_key_v2
    def get(self, table_name):
        parser = tables_fields_argparsers[table_name]['get']
        args = parser.parse_args(strict=True)
        session = Session(engine)
        table = tables[table_name]
        where_clauses = [table.c[key]==value for (key, value) in args.items()]
        result = session.query(table).filter(*where_clauses)
        check_for_empty_table(result)
        # Еще один способ отфильтровать, может пригодится
        # users.update().where(and_(*where_clauses)).values(**update[1])
        columns = table.columns.keys()
        d = [{c: v for c, v in zip(columns, row)} for row in result]
        return jsonify({"data": d})

    @check_api_key_v2
    def put(self, table_name):
        parser = tables_fields_argparsers[table_name]['put']
        args = parser.parse_args(strict=True)
        session = Session(engine)
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

    @check_api_key_v2
    def delete(self, table_name):
        parser = tables_fields_argparsers[table_name]['del']
        args = parser.parse_args(strict=True)
        session = Session(engine)
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


app = Flask(__name__)
app.json_provider_class = CustomJSONEncoder
api = Api(app)
api.add_resource(Table, '/clc/api/v1/<table_name>')
# Если таблицы нет, то выдает ошибку 500, нужно 404


if __name__ == '__main__':
    app.run(debug=False)







