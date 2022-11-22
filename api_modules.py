

from sqlalchemy import create_engine, inspect
from flask_restful import reqparse
from sqlalchemy.ext.automap import automap_base
from datetime import date, datetime
from flask.json import JSONEncoder
from loguru import logger
# from sqlalchemy.orm import declarative_base


# def create_db_resources(creds):
#     conn_str = "mysql+pymysql://{username}:{password}@{hostname}/{dbname}".format(**creds)
#     engine = create_engine(conn_str, echo=False)
#     Base = automap_base()
#     Base.prepare(engine, reflect=True)
#     tables = Base.metadata.tables
#     return engine, tables

def build_actions_argparsers():
    parsers = {}
    parsers['give_clc_id_to_ek'] = reqparse.RequestParser()
    parsers['give_clc_id_to_ek'].add_argument(
        'ek_ids', required=True, nullable=False, store_missing=False, type=int, action='append')
    parsers['give_clc_id_to_ek'].add_argument(
        'clc_id', required=False, nullable=True, store_missing=True, type=int, action='store')
    return parsers


def build_spec_argparsers():
    parsers = {}

    parsers['ek_mats'] = reqparse.RequestParser()
    parsers['ek_mats'].add_argument(
        'ek_id', required=True, nullable=False, store_missing=False, type=int)

    return parsers

    
def create_db_resources_v2(creds):
    engine = {}
    for k, v in creds.items():
        conn_str = "mysql+pymysql://{username}:{password}@{hostname}/{dbname}".format(**v)
        engine[k] = create_engine(conn_str, echo=False)
    Base = automap_base()
    Base.prepare(engine['production'], reflect=True)
    tables = Base.metadata.tables
    return engine, tables


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


# tables_fields_argparsers – это словарь (объект), содержащий парсеры аргументов запроса,
# которые соответствуют полям таблиц в существующей БД
# Инициализируется перед запуском API затем, чтобы при добавлении или удалении полей в БД,
# API продолжал работать стабильно на старой схеме. Если новые/удаленные поля повлияли на работу системы,
# то мы увидим это через ошибки в API
def build_tables_fields_argparsers(engine, tables, db_name):
    inspector = inspect(engine)
    # Супер полезный код чтобы просмотреть свойства полей всех таблиц во всех БД
    # for schema in schemas:
    #     print("schema: %s" % schema)
    #     for table_name in inspector.get_table_names(schema=schema):
    #         for column in inspector.get_columns(table_name, schema=schema):
    #             print("Column: %s" % column)
    tables_fields_argparsers = {}
    for table_name in inspector.get_table_names(schema=db_name):
        # Для добавления обязательны поля, которые не могут быть пустыми и не имеют
        # автозаполнения либо значения по умолчанию
        parser_put = reqparse.RequestParser()
        # Для фильтрации все поля опциональны
        parser_get = reqparse.RequestParser()
        # Для удаления обязательны те поля, которые образуют уникальный ключ.
        # Иногда это одна колонка ид, иногда – несколько колонок
        parser_delete = reqparse.RequestParser()
        # Для обновления обязательны те поля, которые образуют уникальный ключ, остальные опциональны
        parser_update = reqparse.RequestParser()
        table = tables[table_name]
        primary_keys = []
        for column in inspect(table).primary_key:
            # column.type - тип данных в колонке
            primary_keys.append(column.name)
            parser_delete.add_argument(column.name, required=True, nullable=False, store_missing=False)
            parser_update.add_argument(column.name, required=True, nullable=False, store_missing=False)
        for column in inspector.get_columns(table_name, schema=db_name):
            # Добавить проверку по типу данных ОБЯЗАТЕЛЬНО!
            parser_put.add_argument(
                column['name'],
                # type= # Доделать сопоставлением типов данных возвращаемых схемой SQL с питоновыми типами
                required=not column["nullable"] and \
                    ((not column["autoincrement"]) if "autoincrement" in column else True) and \
                        column['default'] is None,
                nullable=False,
                store_missing=False
                # default=column['default'] # бесполезная штука, потому что все равно тип данных не тот, конвертировать не за чем если БД сразу нужное значение вставит
            )
            parser_get.add_argument(column['name'], required=False, nullable=True, store_missing=False)
            if column['name'] not in primary_keys:
                parser_update.add_argument(
                    column['name'],
                    required=False, # Если не передан, то возвращает ошибку
                    nullable=True, # Если передано None, то возвращает ошибку
                    store_missing=False # Если False, то парсит только переданные значения, остальные нет.
                    # Если True (по дефолту), то все непереданные аргументы парсятся со значениями None
                    )
        # if table_name == 'contractor':
        #     logger.debug(parser_update.args)
        # for i in parser_update.args:
        # logger.debug([a.name for a in parser_update.args if a.required])
        tables_fields_argparsers[table_name] = {
            'get': parser_get,
            'put': parser_put,
            'del': parser_delete,
            'upd': parser_update
        }
    return tables_fields_argparsers


