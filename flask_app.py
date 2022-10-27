from flask import Flask, request, jsonify, make_response #, Response, jsonify, 
from flask.json import JSONEncoder
from flask_restful import Resource, Api, reqparse, abort #, fields, marshal_with
from loguru import logger
import pandas as pd
from sqlalchemy import create_engine
from sqlalchemy.orm import Session
import json
from sqlalchemy.ext.automap import automap_base
from datetime import date
from sqlalchemy.orm import declarative_base
from sqlalchemy import inspect


creds = {
    "hostname": "194.67.116.213",
    "port": "3306",
    "username": "root",
    "password": "zs$N7b*7F2Zq",
    "dbname": "scandia_clc"
}


conn_str = "mysql+pymysql://{username}:{password}@{hostname}/{dbname}".format(**creds)


def create_db_resources(
    conn_str, return_engine=False, return_session=False,
    return_inspector=False, return_tables=False, table_name='ALL'):
    lst_to_return = []
    engine = create_engine(conn_str, echo=False)
    if return_engine:
        lst_to_return.append(engine)
    if return_session:
        lst_to_return.append(Session(engine))
    if return_inspector:
        lst_to_return.append(inspect(engine))
    if return_tables:
        Base = automap_base()
        Base.prepare(engine, reflect=True)
        # Аналог automap_base, различия выяснить
        # Base = declarative_base()
        # Base.metadata.reflect(engine)
        tables = Base.metadata.tables
        if table_name != 'ALL':
            tables = tables[table_name]
        lst_to_return.append(tables)
    if not lst_to_return:
        raise Exception('Функция создания ресурсов для работы с БД должна вернуть хотя бы один из них!')
    return lst_to_return if len(lst_to_return) > 1 else lst_to_return[0]


def build_tables_fields_argparsers(conn_str):
    inspector, tables = create_db_resources(conn_str, return_inspector=True, return_tables=True)
    # Супер полезный код чтобы просмотреть свойства полей всех таблиц во всех БД
    # for schema in schemas:
    #     print("schema: %s" % schema)
    #     for table_name in inspector.get_table_names(schema=schema):
    #         for column in inspector.get_columns(table_name, schema=schema):
    #             print("Column: %s" % column)
    tables_fields_argparsers = {}
    for table_name in inspector.get_table_names(schema=creds['dbname']):
        # Для добавления обязательны поля, которые не могут быть пустыми и не имеют
        # автозаполнения либо значения по умолчанию
        parser_put = reqparse.RequestParser()
        # Для фильтрации все поля опциональны
        parser_get = reqparse.RequestParser()
        # Для удаления обязательны те поля, которые образуют уникальный ключ.
        # Иногда это одна колонка ид, иногда – несколько колонок
        parser_delete = reqparse.RequestParser()
        # if table_name != 'contractors':
        #     continue
        table = tables[table_name]
        for column in inspect(table).primary_key:
            # column.type - тип данных в колонке
            parser_delete.add_argument(column.name, required=True)
        for column in inspector.get_columns(table_name, schema=creds['dbname']):
            # Добавить проверку по типу данных ОБЯЗАТЕЛЬНО!
            parser_put.add_argument(
                column['name'],
                # type= # Доделать сопоставлением типов данных возвращаемых схемой SQL с питоновыми типами
                required=not column["nullable"] and \
                    ((not column["autoincrement"]) if "autoincrement" in column else True) and \
                        column['default'] is None,
                # default=column['default'] # бесполезная штука, потому что все равно тип данных не тот, конвертировать не за чем если БД сразу нужное значение вставит
            )
            parser_get.add_argument(column['name'], required=False)
        tables_fields_argparsers[table_name] = {
            'get': parser_get,
            'put': parser_put,
            'del': parser_delete
        }
    return tables_fields_argparsers


# tables_fields_argparsers – это словарь (объект), содержащий парсеры аргументов запроса,
# которые соответствуют полям таблиц в существующей БД
# Инициализируется перед запуском API затем, чтобы при добавлении или удалении полей в БД,
# API продолжал работать стабильно на старой схеме. Если новые/удаленные поля повлияли на работу системы,
# то мы увидим это через ошибки в API
tables_fields_argparsers = build_tables_fields_argparsers(conn_str)


class CustomJSONEncoder(JSONEncoder):
    def default(self, obj):
        try:
            if isinstance(obj, date):
                return obj.isoformat()
            iterable = iter(obj)
        except TypeError:
            pass
        else:
            return list(iterable)
        return JSONEncoder.default(self, obj)


def get_table_arguments(method, table_name):
    # Здесь распознаются только те аргументы, соответсвующие колонкам таблиц, остальные игнорируются.
    # Нужно доделать предупреждение
    args = tables_fields_argparsers[table_name][method].parse_args()
    args = {k: v for k, v in args.items() if v is not None}
    return args


class Table(Resource):
    def get(self, table_name):
        args = get_table_arguments('get', table_name)
        session, table = create_db_resources(conn_str, return_session=True, return_tables=True, table_name=table_name)
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
        session, table = create_db_resources(conn_str, return_session=True, return_tables=True, table_name=table_name)
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
        session, table = create_db_resources(conn_str, return_session=True, return_tables=True, table_name=table_name)
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
app.json_encoder = CustomJSONEncoder
api = Api(app)
api.add_resource(Table, '/clc/api/v1/<table_name>')



if __name__ == '__main__':
    app.run(debug=True)







