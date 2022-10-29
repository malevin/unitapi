from flask import Flask, jsonify, make_response #, Response, jsonify, 
from flask_restful import Resource, Api, reqparse, abort #, fields, marshal_with
from loguru import logger
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


def check_api_key(key):
    if key != KEY:
        abort(401, message='Unauthorized')


def get_table_arguments(method, table_name):
    # strict=True означает, что аргументы, которые не удалось распознать вызовут ошибку запроса
    args = tables_fields_argparsers[table_name][method].parse_args(strict=True)
    args = {k: v for k, v in args.items() if v is not None}
    check_api_key(args['key'])
    # Убираем ключ из аргументов
    args.pop('key', None)
    return args


class Table(Resource):
    # @check_api_key
    def get(self, table_name):
        args = get_table_arguments('get', table_name)
        logger.debug(args)
        # logger.debug(args.data)
        session = Session(engine)
        table = tables[table_name]
        where_clauses = [table.c[key]==value for (key, value) in args.items()]
        result = session.query(table).filter(*where_clauses)
        # Еще один способ отфильтровать, может пригодится
        # q = q.filter(getattr(myClass, attr).like("%%%s%%" % value))
        # users.update().where(and_(*where_clauses)).values(**update[1])
        columns = table.columns.keys()
        d = [{c: v for c, v in zip(columns, row)} for row in result]
        return jsonify({"data": d})

    def put(self, table_name):
        args = get_table_arguments('put', table_name)
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

    def delete(self, table_name):
        args = get_table_arguments('del', table_name)
        session = Session(engine)
        table = tables[table_name]
        try:
            where_clauses = [table.c[key]==value for (key, value) in args.items()]
            session.query(table).filter(*where_clauses).delete()
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
    app.run(debug=True)







