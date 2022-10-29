

from sqlalchemy import create_engine, inspect
from flask_restful import reqparse
from sqlalchemy.ext.automap import automap_base
from datetime import date, datetime
from flask.json import JSONEncoder
# from sqlalchemy.orm import declarative_base


def create_db_resources(creds):
    conn_str = "mysql+pymysql://{username}:{password}@{hostname}/{dbname}".format(**creds)
    engine = create_engine(conn_str, echo=False)
    Base = automap_base()
    Base.prepare(engine, reflect=True)
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
        parser_put.add_argument('key', required=True, location='headers')
        # Для фильтрации все поля опциональны
        parser_get = reqparse.RequestParser()
        parser_get.add_argument('key', required=True, location='headers')
        # Для удаления обязательны те поля, которые образуют уникальный ключ.
        # Иногда это одна колонка ид, иногда – несколько колонок
        parser_delete = reqparse.RequestParser()
        parser_delete.add_argument('key', required=True, location='headers')
        # if table_name != 'contractors':
        #     continue
        table = tables[table_name]
        for column in inspect(table).primary_key:
            # column.type - тип данных в колонке
            parser_delete.add_argument(column.name, required=True)
        for column in inspector.get_columns(table_name, schema=db_name):
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
        if table_name == 'contractor':
            logger.debug(parser_get.args)
        tables_fields_argparsers[table_name] = {
            'get': parser_get,
            'put': parser_put,
            'del': parser_delete
        }
    return tables_fields_argparsers


