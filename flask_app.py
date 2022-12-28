from flask import Flask, jsonify, make_response, request
from flask_restful import Resource, Api, reqparse, abort
from loguru import logger
import jwt
from functools import wraps
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session, load_only
from sqlalchemy.sql import text
from sqlalchemy.ext.declarative import DeclarativeMeta
import json
from sqlalchemy import create_engine, inspect, or_, and_
from flask_restful import reqparse
from sqlalchemy.ext.automap import automap_base
from datetime import date, datetime
from flask.json import JSONEncoder
from bcrypt import checkpw
from datetime import datetime, timedelta, timezone
import copy
from flask_json import FlaskJSON
from pprint import pprint
from api_modules import build_init_tables_argparsers, create_db_resources_v3, build_spec_argparsers, build_actions_argparsers
import warnings
warnings.simplefilter(action='ignore', category=FutureWarning)
pd.options.mode.chained_assignment = None
# from sqlalchemy.orm imposrt declarative_base


KEY = '89a10379-1373-4a2e-b331-0adc36157443'
creds = {
    'auth': {
        'production': {
            "hostname": "194.67.116.213",
            "port": "3306",
            "username": "root",
            "password": "zs$N7b*7F2Zq",
            "dbname": "auth_db"
        }
    },
    'clc': {
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
    },
    'uu': {
        'scandia': {
            "hostname": "194.67.116.213",
            "port": "3306",
            "username": "root",
            "password": "zs$N7b*7F2Zq",
            "dbname": "scandia_UU"
        },
        'spv': {
            "hostname": "194.67.116.213",
            "port": "3306",
            "username": "root",
            "password": "zs$N7b*7F2Zq",
            "dbname": "spv_UU"
        },
        'unit': {
            "hostname": "194.67.116.213",
            "port": "3306",
            "username": "root",
            "password": "zs$N7b*7F2Zq",
            "dbname": "unit_UU"
        },
        'unitgrad': {
            "hostname": "194.67.116.213",
            "port": "3306",
            "username": "root",
            "password": "zs$N7b*7F2Zq",
            "dbname": "unitgrad_UU"
        },
        'mkd': {
            "hostname": "194.67.116.213",
            "port": "3306",
            "username": "root",
            "password": "zs$N7b*7F2Zq",
            "dbname": "mkd_UU"
        },
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
parsers['initial'] = build_init_tables_argparsers(engines, db_tables, creds)
parsers['actions'] = build_actions_argparsers(creds)
parsers['special'] = build_spec_argparsers(creds)


def check_developers_token(function=None):
    @wraps(function)
    def wrapper(*args, **kwargs):
        h = dict(request.headers)
        if 'Token' not in h:
            abort(400, message='Missing token in headers')
        try:
            res = jwt.decode(h['Token'], KEY, algorithms="HS256")
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
        except Exception as error:
            abort(401, message=str(error))
        res = function(*args, **kwargs)
        return res
    return wrapper


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


def check_for_empty_table(q, multiple_records_abort=False):
    c = q.count()
    if c == 0:
        abort(404, message='Record is not found')
    if c > 1 and multiple_records_abort:
        abort(400, message='Multiple records found. Ask developers to check indexes in database and required parameters in API')


class Table(Resource):
    @check_token
    @get_init_table_args
    def put(self, session, table, args, primary_keys):
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
            session.execute(table.insert(), insert_list)
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


def update_eks_clc_id(session, tables, args):
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


def update_mats_spc_id(session, tables, args):
    r_ek_basic_materials = tables['r_ek_basic_materials']
    r_ek_add_materials = tables['r_ek_add_materials']
    if args['r_ek_add_mats_ids'] is None and args['r_ek_basic_mats_ids'] is None:
        abort(400, message='At least one basic or additional material must be passed in request')
    try:
        if args['r_ek_basic_mats_ids'] is not None:
            session.query(r_ek_basic_materials).filter(r_ek_basic_materials.c['id'].in_(args['r_ek_basic_mats_ids'])).update({'spc_id': args['spc_id']})
        if args['r_ek_add_mats_ids'] is not None:
            session.query(r_ek_add_materials).filter(r_ek_add_materials.c['id'].in_(args['r_ek_add_mats_ids'])).update({'spc_id': args['spc_id']})
        session.commit()
        return '', 204
    except Exception as error:
        session.rollback()
        response = make_response(jsonify(
            {'error': str(error)}
        ), 403)
        return response


def delete_ek_with_mats(session, tables, ek_ids):
    ek = tables['ek']
    r_ek_basic_mats = tables['r_ek_basic_materials']
    r_ek_add_mats = tables['r_ek_add_materials']
    try:
        session.query(r_ek_add_mats).filter(r_ek_add_mats.c['ek_id'].in_(ek_ids)).delete()
        session.query(r_ek_basic_mats).filter(r_ek_basic_mats.c['ek_id'].in_(ek_ids)).delete()
        session.query(ek).filter(ek.c['id'].in_(ek_ids)).delete()
        session.commit()
        return '', 204
    except Exception as error:
        session.rollback()
        response = make_response(jsonify(
            {'error': str(error)}
        ), 403)
        return response


def delete_clc_with_eks(session, tables, clc_ids):
    clc = tables['clc']
    ek = tables['ek']
    try:
        session.query(ek).filter(ek.c['clc_id'].in_(clc_ids)).update({'clc_id': None})
        session.query(clc).filter(clc.c['id'].in_(clc_ids)).delete()
        session.commit()
        return '', 204
    except Exception as error:
        session.rollback()
        response = make_response(jsonify(
            {'error': str(error)}
        ), 403)
        return response


def delete_spc_with_mats(session, tables, spc_ids):
    spc = tables['spc']
    r_ek_basic_mats = tables['r_ek_basic_materials']
    r_ek_add_mats = tables['r_ek_add_materials']
    try:
        session.query(r_ek_basic_mats).filter(r_ek_basic_mats.c['spc_id'].in_(spc_ids)).update({'spc_id': None})
        session.query(r_ek_add_mats).filter(r_ek_add_mats.c['spc_id'].in_(spc_ids)).update({'spc_id': None})
        session.query(spc).filter(spc.c['id'].in_(spc_ids)).delete()
        session.commit()
        return '', 204
    except Exception as error:
        session.rollback()
        response = make_response(jsonify(
            {'error': str(error)}
        ), 403)
        return response


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
                # Ищем парсер специально для любой БД в этом продукте
                psr = parsers[branch][product]['COMMON'][resource_name]
            except:
                try:
                    # Ищем парсер специально для любой БД любого продукта
                    psr = parsers[branch]['COMMON'][resource_name]
                except:
                    abort(404, message='Action or special table not found')
        prs_args = psr.parse_args(strict=True)
        kw = {'eng': eng, 'session': session, 'tables': tables, 'args': prs_args, 'resource_name': resource_name}
        res = function(*args, **kw)
        return res
    return wrapper


def approve_payment_requests(session, tables, args):
    pr = tables['payment_requests']
    try:
        session.query(pr).filter(pr.c['id'].in_(args['pr_ids'])).update({'approved_by_' + args['approve_by']: 1})
        session.commit()
        return '', 204
    except Exception as error:
        session.rollback()
        response = make_response(jsonify(
            {'error': str(error)}
        ), 403)
        return response


def decline_payment_requests(session, tables, args):
    pr = tables['payment_requests']
    try:
        session.query(pr).filter(pr.c['id'].in_(args['pr_ids'])).update({'approved_by_' + args['decline_by']: 0})
        session.commit()
        return '', 204
    except Exception as error:
        session.rollback()
        response = make_response(jsonify(
            {'error': str(error)}
        ), 403)
        return response


def set_payment_requests_into_pack(session, tables, args):
    pr = tables['payment_requests']
    try:
        session.query(pr).filter(pr.c['id'].in_(args['pr_ids'])).update({'payment_requests_packs_id': args['pack_id']})
        session.commit()
        return '', 204
    except Exception as error:
        session.rollback()
        response = make_response(jsonify(
            {'error': str(error)}
        ), 403)
        return response


def create_pack_with_payment_requests(session, tables, args):
    pr = tables['payment_requests']
    packs = tables['payment_requests_packs']
    try:
        # ans = session.query(packs).insert({'date': args['date'], 'number': args['number']})
        ans = session.execute(packs.insert({'date': args['date'], 'number': args['number']}), )
        last_id = session.query(packs).order_by(packs.c['id'].desc()).first()[0]
        session.query(pr).filter(pr.c['id'].in_(args['pr_ids'])).update({'payment_requests_packs_id': last_id})
        session.commit()
        return make_response(jsonify({'created_pack_id': last_id}), 200)
    except Exception as error:
        session.rollback()
        response = make_response(jsonify(
            {'error': str(error)}
        ), 403)
        return response


def delete_pack_with_payment_requests(session, tables, args):
    pr = tables['payment_requests']
    packs = tables['payment_requests_packs']
    try:
        session.query(pr).filter(pr.c['payment_requests_packs_id'] == args['pack_id']).update({'payment_requests_packs_id': None})
        session.query(packs).filter(packs.c['id'] == args['pack_id']).delete()
        session.commit()
        return '', 204
    except Exception as error:
        session.rollback()
        response = make_response(jsonify(
            {'error': str(error)}
        ), 403)
        return response


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
        elif resource_name == 'format_estimation_json_for_print':
            json_data = format_estimation_json_for_print(eng, session, tables, args['est_id'])
            response = make_response(jsonify(json_data), 200)
            response.headers["Content-Type"] = "application/json"
            return response


def get_df_from_db(eng, session, tables, table_name, filters):
    t = tables[table_name]
    columns = t.columns.keys()
    filters = [
        t.c[col].in_(
            [v] if not isinstance(v, (list, tuple, set, pd.Series)) else v
        ) for col, v in filters.items()
    ]
    t = session.query(t).filter(*filters)
    t = pd.read_sql(t.statement, eng)
    return t


def get_table_from_db(session, tables, table_name, filters, remain_cols=None, add_prefix=False):
    t = tables[table_name]
    columns = t.columns.keys()
    if add_prefix:
        columns = [table_name + '_' + c for c in columns]
        if remain_cols is not None:
            remain_cols = [table_name + '_' + c for c in remain_cols]
    filters = [
        t.c[col].in_(
            [v] if not isinstance(v, (list, tuple, set, pd.Series)) else v
        ) for col, v in filters.items()
    ]
    result = session.query(t).filter(*filters)
    if remain_cols is not None:
        t = [{c: v for c, v in zip(columns, row) if c in remain_cols} for row in result]
    else:
        t = [{c: v for c, v in zip(columns, row)} for row in result]
    return t


def format_estimation_json_for_print(eng, session, tables, est_id, debug_flag=False):
    est = get_table_from_db(session, tables, 'estimations', {'id': est_id})[0]
    # {
    #     "works_sum": 10000,
    #     "materials_sum": 10000,
    #     "item_clc_code" : "4.4",
    #     "item_name" : "Какая-то статья",
    #     "object_full_name" : "Оооооочень длинный текст",
    #     "contract_name" : "№ 1 от 22.12.2022 Выполнение работ",
    #     "estimation_name" : "Расчет 1",
    #     "start_date" : "21.11.2022",
    #     "end_date" : "22.12.2022",
    #     "contract_prepayment" : 100000,
    #     "work_types_description" : "Тут должно быть описание работы"
    # }
    json_data = {
        "estimation_id" : est_id,
        "ss_id": est['ss_id'],
        "props": None,
        "eps" : None
    }
    ek = get_df_from_db(eng, session, tables, 'ek', {'estimation_id': est_id})
    if ek.empty and not debug_flag:
        abort(404, message='Документ не содержит ни одной работы')

    ep = get_df_from_db(eng, session, tables, 'ep', {'id': ek.ep_id})
    logger.debug(f'ek:\n{ek}')
    logger.debug(ek.clc_id.dropna())
    clc_works_prices = get_df_from_db(eng, session, tables, 'clc_works_prices', {'clc_id': ek.clc_id.dropna()})
    logger.debug(clc_works_prices)
    work_types = get_df_from_db(eng, session, tables, 'work_types', {'id': ek.work_types_id})

    ek = ek.merge(clc_works_prices, how='left', on=['clc_id', 'work_types_id'])
    
    ek = ek.merge(work_types, how='left', left_on='work_types_id', right_on='id',suffixes=(None, '_work_types'))

    ek['works_price'] = ek['price'].fillna(ek['unit_price'])
    # logger.debug(ek[['id', 'price', 'unit_price']])
    ek['works_cost'] = ek.works_price * ek.volume

    materials = make_est_materials_table(eng, session, tables, est_id)
    logger.debug(materials)
    base_mats = materials[[
        'id', 'ek_id', 'materials_id', 'name', 'ed_izm',
        'consumption_rate', 'overconsumption', 'price', 'cost', 'volume'
    ]].loc[materials.is_basic]
    add_mats = materials[[
        'id', 'ek_id', 'materials_id', 'name', 'ed_izm', 'price', 'cost', 'volume'
    ]].loc[np.logical_not(materials.is_basic)]

    ek = ek[['id', 'work_types_id', 'volume', 'name', 'ed_izm', 'works_cost', 'ep_id', 'works_price']]
    logger.debug(ek)
    mats_summary = materials[['ek_id', 'cost']].rename(columns={'cost': 'materials_cost'})#.sum()
    
    # Если материалов в расчете нет, то после groupby(...).sum() клонки materials_cost не останется
    if not mats_summary.empty:
        mats_summary = mats_summary.groupby('ek_id', as_index=False).sum()
    logger.debug(f'mats_summary:\n{mats_summary}')
    ek = ek.merge(mats_summary, how='left', left_on='id', right_on='ek_id')
    ek.drop(columns=['ek_id'], inplace=True)
    ek['materials_cost'] = ek['materials_cost'].fillna(0)
    ek['cost'] = ek['materials_cost'] + ek['works_cost']
    logger.debug(ek)
    # raise Exception

    eks_summary = ek[['ep_id', 'volume', 'materials_cost', 'works_cost', 'cost']].groupby('ep_id', as_index=False).sum()
    ep = ep.merge(eks_summary, how='left', left_on='id', right_on='ep_id')
    ep['price'] = ep['cost'] / ep['volume']
    logger.debug(ep)

    ep_list = ep[['id', 'name', 'price', 'volume', 'cost']].to_dict('records')

    for i, ep_row in enumerate(ep_list):
        ep_eks = ek.loc[ek.ep_id == ep_row['id']]
        ek_list = ep_eks.drop(columns=['ep_id']).to_dict('records')
        for j, ek_row in enumerate(ek_list):
            ek_base_mat = base_mats.loc[base_mats.ek_id == ek_row['id']]
            ek_base_mat_list = ek_base_mat.drop(columns=['ek_id']).to_dict('records')
            ek_add_mat = add_mats.loc[add_mats.ek_id == ek_row['id']]
            ek_add_mat_list = ek_add_mat.drop(columns=['ek_id']).to_dict('records')
            ek_list[j]['base_mats'] = ek_base_mat_list
            ek_list[j]['add_mats'] = ek_add_mat_list
            ek_list[j]['order_num'] = str(i+1) + '.' + str(j+1)
        ep_list[i]['eks'] = ek_list
        ep_list[i]['order_num'] = str(i+1)
    
    json_data['eps'] = ep_list

    objects = get_table_from_db(
        session, tables, 'objects', {'id': est['objects_id']},
        ['full_name', 'short_name'], True
    )[0]
    items = get_table_from_db(
        session, tables, 'items', {'id': est['items_id']},
        ['name', 'clc_code'], True
    )[0]

    # contracts = get_table_from_db(
    #     session, tables, 'contracts', {'id': est['items_id']}, ['name', 'number']
    # )[0]
    props = ep[['materials_cost', 'works_cost', 'cost', 'volume']].sum().squeeze().to_dict()
    props = {
        **{k: v for k, v in est.items() if k not in ['id', 'ss_id']},
        **props,
        **items,
        **objects,
        # **{'contracts_name': f"№ {contracts['number']} от {contracts['date']} {contracts_name}"}
    }
    json_data['props'] = props

    # logger.debug()
    # logger.debug(props)
    # pprint(json_data)
    return json_data


def make_est_materials_table(eng, session, tables, est_id):
    est = tables['estimations']
    columns = est.columns.keys()
    est = session.query(est).filter(est.c['id']==est_id).one()
    est = {c: v for c, v in zip(columns, est)}

    ek = tables['ek']
    ek = session.query(ek).filter(
        ek.c['estimation_id']==est_id)
    ek = pd.read_sql(ek.statement, eng)
    
    # logger.debug(f'est: {est}')

    r_work_types_basic_materials = tables['r_work_types_basic_materials']
    r_work_types_basic_materials = session.query(r_work_types_basic_materials).filter(
        r_work_types_basic_materials.c['work_types_id'].in_(ek['work_types_id']))
    r_work_types_basic_materials = pd.read_sql(r_work_types_basic_materials.statement, eng)
    # logger.debug(r_work_types_basic_materials)

    r_ek_basic_materials = tables['r_ek_basic_materials']
    r_ek_basic_materials = session.query(r_ek_basic_materials).filter(r_ek_basic_materials.c['ek_id'].in_(ek.id))
    r_ek_basic_materials = pd.read_sql(r_ek_basic_materials.statement, eng)
    # r_ek_basic_materials = r_ek_basic_materials.merge(r_work_types_basic_materials, how='left', on='materials_id')
    # r_ek_basic_materials['volume'] = r_ek_basic_materials['consumption_rate'] * ek['volume']
    r_ek_basic_materials['is_basic'] = True
    
    r_ek_add_materials = tables['r_ek_add_materials']
    r_ek_add_materials = session.query(r_ek_add_materials).filter(r_ek_add_materials.c['ek_id'].in_(ek.id))
    r_ek_add_materials = pd.read_sql(r_ek_add_materials.statement, eng)
    r_ek_add_materials['is_basic'] = False

    df = pd.concat([r_ek_basic_materials, r_ek_add_materials], axis=0)

    materials = tables['materials']
    materials = session.query(materials).filter(
        materials.c['id'].in_(df['materials_id']))
    materials = pd.read_sql(materials.statement, eng)
    df = df.merge(materials, how='left', left_on='materials_id', right_on='id', suffixes=[None, '_mat'])
    df = df.merge(ek, how='left', left_on='ek_id', right_on='id', suffixes=[None, '_ek'])
    df = df.merge(r_work_types_basic_materials, how='left', left_on=['work_types_id', 'materials_id'], right_on=['work_types_id', 'materials_id'])
    df['volume'].loc[df['is_basic']] = df['consumption_rate'] * df['volume_ek']

    mats = df.materials_id.unique().tolist()

    prices_history = tables['materials_prices_history']
    prices_history = session.query(prices_history).filter(
        and_(
            prices_history.c['materials_id'].in_(mats),
            or_(
                prices_history.c['objects_id'] == est['objects_id'],
                prices_history.c['objects_id'].is_(None)
            )
        )
    )
    

    prices_history = pd.read_sql(prices_history.statement, eng)
    # Логику цен переделать!
    logger.debug(prices_history.loc[prices_history.objects_id.isna()])
    prices_history = prices_history[['materials_id', 'price']]
    prices_history.drop_duplicates(subset=['materials_id'], keep='last', inplace=True)
    df = df.merge(prices_history, how='left', on='materials_id', suffixes=[None, '_mph'])
    df['price'] = df['closed_price'].fillna(df['price'])
    df['cost'] = df.price * df.volume
    df['overconsumption'] = 1
    # df.drop(columns=['price'], inplace=True)

    return df


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


class Auth(Resource):
    @check_token
    def post(self):
        return jsonify({"message": "Token is valid"})

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
            "exp": datetime.now(timezone.utc) + timedelta(hours=500)
        }
        token = jwt.encode(payload_data, KEY)
        return token, 200


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


def debug():
    tables = db_tables['clc']['production']
    eng = engines['clc']['production']
    session = Session(eng)
    format_estimation_json_for_print(eng, session, tables, 10, True)


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
# Если таблицы нет, то выдает ошибку 500, нужно 404


if __name__ == '__main__':
    app.run(debug=True)
    # debug()







