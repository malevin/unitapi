

from sqlalchemy import create_engine, inspect
from flask_restful import reqparse
from sqlalchemy.ext.automap import automap_base
from datetime import date, datetime
from flask.json import JSONEncoder
from loguru import logger
import copy
# from sqlalchemy.orm import declarative_base


# def create_db_resources(creds):
#     conn_str = "mysql+pymysql://{username}:{password}@{hostname}/{dbname}".format(**creds)
#     engine = create_engine(conn_str, echo=False)
#     Base = automap_base()
#     Base.prepare(engine, reflect=True)
#     tables = Base.metadata.tables
#     return engine, tables

def build_actions_argparsers(creds):
    actions_parsers = copy.deepcopy(creds)
    for product, dbs in creds.items():
        for db in dbs.keys():
            actions_parsers[product][db] = {}
            # Ключ словаря для хранения парсеров, общих для всех БД в рамках одного продукта, например для всех баз данных УУ
            actions_parsers[product]['COMMON'] = {}
    # Ключ словаря для хранения парсеров, общих для всех БД во всех продуктах
    actions_parsers['COMMON'] = {}

    ps = reqparse.RequestParser()
    ps.add_argument(
        'ek_ids', required=True, nullable=False, store_missing=False, type=int, action='append')
    ps.add_argument(
        'clc_id', required=False, nullable=True, store_missing=True, type=int, action='store')
    actions_parsers['clc']['COMMON']['give_clc_id_to_ek'] = ps

    ps = reqparse.RequestParser()
    ps.add_argument(
        'r_ek_basic_mats_ids', required=False, nullable=True, store_missing=True, type=int, action='append')
    ps.add_argument(
        'r_ek_add_mats_ids', required=False, nullable=True, store_missing=True, type=int, action='append')
    ps.add_argument(
        'spc_id', required=False, nullable=True, store_missing=True, type=int, action='store')
    actions_parsers['clc']['COMMON']['give_spc_id_to_material'] = ps

    ps = reqparse.RequestParser()
    ps.add_argument(
        'email', required=True, nullable=False, store_missing=False, type=str)
    ps.add_argument(
        'password', required=True, nullable=False, store_missing=False, type=str)
    actions_parsers['auth']['COMMON']['check_pwd'] = ps

    # Специальные удаления ЕК, спецификаций и калькуляций написаны так, чтобы можно было передать несколько айди сущностей для удаления списком
    ps = reqparse.RequestParser()
    ps.add_argument(
        'ek_ids', required=True, nullable=False, store_missing=False, type=int, action='append')
    actions_parsers['clc']['COMMON']['delete_ek_with_mats'] = ps

    ps = reqparse.RequestParser()
    ps.add_argument(
        'clc_ids', required=True, nullable=False, store_missing=False, type=int, action='append')
    actions_parsers['clc']['COMMON']['delete_clc_with_eks'] = ps

    ps = reqparse.RequestParser()
    ps.add_argument(
        'spc_ids', required=True, nullable=False, store_missing=False, type=int, action='append')
    actions_parsers['clc']['COMMON']['delete_spc_with_mats'] = ps

    ps = reqparse.RequestParser()
    ps.add_argument(
        'query', required=True, nullable=False, store_missing=False, type=str, action='append')
    actions_parsers['COMMON']['sql'] = ps

    return actions_parsers


def build_spec_argparsers(creds):
    spec_parsers = copy.deepcopy(creds)
    for product, dbs in creds.items():
        for db in dbs.keys():
            spec_parsers[product][db] = {}
            # Ключ словаря для хранения парсеров, общих для всех БД в рамках одного продукта, например для всех баз данных УУ
            spec_parsers[product]['COMMON'] = {}
    
    # Все материалы по расчету
    ps = reqparse.RequestParser()
    ps.add_argument(
        'est_id', required=True, nullable=False, store_missing=False, type=int)
    spec_parsers['clc']['COMMON']['est_mats'] = ps

    return spec_parsers


def create_db_resources_v3(creds):
    engines = copy.deepcopy(creds)
    tables = copy.deepcopy(creds)
    inspectors = copy.deepcopy(creds)
    for product, dbs in creds.items():
        # ___________________
        if product not in ['clc', 'auth']:
            continue
        # ___________________
        for db, data in dbs.items():
            if product == 'clc' and db != 'production':
                continue
            # logger.debug(f'{product} - {db} - {data}')
            conn_str = "mysql+pymysql://{username}:{password}@{hostname}/{dbname}".format(**data)
            eng = create_engine(conn_str, echo=False)
            logger.debug(eng.url.database)
            Base = automap_base()
            Base.prepare(eng, reflect=True)
            engines[product][db] = eng
            tables[product][db] = Base.metadata.tables
            inspectors[product][db] = inspect(eng)
    return engines, tables, inspectors


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
def build_init_tables_argparsers(engines, tables, creds):
    tables_fields_argparsers = copy.deepcopy(creds)
    for product, dbs in engines.items():
        # ___________________
        if product not in ['clc', 'auth']:
            continue
        # ___________________
        for db, eng in dbs.items():
            if product == 'clc' and db != 'production':
                continue
            tables_fields_argparsers[product][db] = {}
            # Дефолтные парсеры для ообращения непосредственно к таблицам
            inspector = inspect(eng)
            for table_name in inspector.get_table_names(schema=eng.url.database):
                if table_name != 'clc':
                    continue
                table_parsers = {k: reqparse.RequestParser() for k in [
                    'PUT', # Для добавления обязательны поля, которые не могут быть пустыми и не имеют автозаполнения либо значения по умолчанию
                    'DELETE', # Для удаления обязательны те поля, которые образуют уникальный ключ.
                    'GET', # Для фильтрации все поля опциональны
                    'POST'  # Для обновления обязательны те поля, которые образуют уникальный ключ, остальные опциональны
                ]}
                table = tables[product][db][table_name]
                primary_keys = []
                for column in inspect(table).primary_key:
                    # column.type - тип данных в колонке
                    primary_keys.append(column.name)
                    table_parsers['DELETE'].add_argument(column.name, required=True, nullable=False, store_missing=False)
                    table_parsers['PUT'].add_argument(column.name, required=True, nullable=False, store_missing=False)
                for column in inspector.get_columns(table_name, schema=eng.url.database):
                    # Добавить проверку по типу данных ОБЯЗАТЕЛЬНО!
                    # req_f = not column["nullable"] and \
                    #     ((not column["autoincrement"]) if "autoincrement" in column else True) and \
                    #         column['default'] is None
                    table_parsers['POST'].add_argument(
                        column['name'],
                        # type= # Доделать сопоставлением типов данных возвращаемых схемой SQL с питоновыми типами
                        required=not column["nullable"] and \
                            ((not column["autoincrement"]) if "autoincrement" in column else True) and \
                                column['default'] is None,
                        nullable=True,
                        store_missing=False
                        # default=column['default'] # бесполезная штука, потому что все равно тип данных не тот, конвертировать не за чем если БД сразу нужное значение вставит
                    )
                    table_parsers['GET'].add_argument(column['name'], required=False, nullable=True, store_missing=False)
                    if column['name'] not in primary_keys:
                        table_parsers['PUT'].add_argument(
                            column['name'],
                            required=False, # Если не передан, то возвращает ошибку
                            nullable=True, # Если передано None, то возвращает ошибку
                            store_missing=False # Если False, то парсит только переданные значения, остальные нет.
                            # Если True (по дефолту), то все непереданные аргументы парсятся со значениями None
                            )
                # if table_name == 'contractor':
                    # logger.debug(table_parsers['POST'].args)
                logger.debug(table_parsers)
                tables_fields_argparsers[product][db][table_name] = table_parsers
                # logger.debug(tables_fields_argparsers[product][db][table_name])
    return tables_fields_argparsers


# def create_db_resources_v2(creds, auth_creds):
#     conn_str = "mysql+pymysql://{username}:{password}@{hostname}/{dbname}".format(**auth_creds)
#     auth_engine = create_engine(conn_str, echo=False)
#     Base = automap_base()
#     Base.prepare(auth_engine, reflect=True)
#     auth_tables = Base.metadata.tables

#     engine = {}
#     for k, v in creds.items():
#         conn_str = "mysql+pymysql://{username}:{password}@{hostname}/{dbname}".format(**v)
#         engine[k] = create_engine(conn_str, echo=False)
#     Base = automap_base()
#     Base.prepare(engine['production'], reflect=True)
#     tables = Base.metadata.tables
#     return engine, tables, auth_engine, auth_tables