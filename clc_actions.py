from api_modules import get_df_from_db, get_table_from_db
import pandas as pd
from loguru import logger
from sqlalchemy import or_, and_
from flask import jsonify, make_response
import numpy as np
from pprint import pprint


# Функция возвращает json объект для распечатки расчета в гугл таблицы.
# Шаблон распечатки как у калькуляций
def format_estimation_json(eng, session, tables, est_id, debug_flag=False):
    est = get_table_from_db(session, tables, 'estimations', {'id': est_id})[0]
    props = {}
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
    clc_works_prices = get_df_from_db(eng, session, tables, 'clc_works_prices', {'clc_id': ek.clc_id.dropna()})
    work_types = get_df_from_db(eng, session, tables, 'work_types', {'id': ek.work_types_id})
    ek = ek.merge(clc_works_prices, how='left', on=['clc_id', 'work_types_id'])
    ek = ek.merge(work_types, how='left', left_on='work_types_id', right_on='id',suffixes=(None, '_work_types'))
    ek['works_price'] = ek['price'].fillna(ek['unit_price'])
    ek.drop(columns=['price'], inplace=True)
    ek['works_cost'] = ek.works_price * ek.volume
    
    materials = make_est_materials_table(eng, session, tables, ek=ek[['id', 'work_types_id', 'volume', 'clc_id']], est=est)
    base_mats = materials[[
        'id', 'ek_id', 'materials_id', 'name', 'ed_izm',
        'consumption_rate', 'overconsumption', 'price', 'cost', 'volume', 'contractors_id', 'contractors_name', 'source'
    ]].loc[materials.is_basic]
    add_mats = materials[[
        'id', 'ek_id', 'materials_id', 'name', 'ed_izm', 'price', 'cost', 'volume', 'contractors_id', 'contractors_name', 'source'
    ]].loc[np.logical_not(materials.is_basic)]
    ek = ek[['id', 'work_types_id', 'volume', 'clc_id', 'name', 'ed_izm', 'works_cost', 'ep_id', 'works_price']]
    mats_summary = materials[['ek_id', 'cost']].rename(columns={'cost': 'materials_cost'})#.sum()
    # Если материалов в расчете нет, то после groupby(...).sum() клонки materials_cost не останется
    if not mats_summary.empty:
        mats_summary = mats_summary.groupby('ek_id', as_index=False).sum()
    # logger.debug(f'mats_summary:\n{mats_summary}')
    ek = ek.merge(mats_summary, how='left', left_on='id', right_on='ek_id')
    ek.drop(columns=['ek_id'], inplace=True)
    ek['materials_cost'] = ek['materials_cost'].fillna(0)
    ek['cost'] = ek['materials_cost'] + ek['works_cost']
    ek = ek.replace(np.nan, None)
    logger.debug(ek)
    
    eks_summary = ek[['ep_id', 'volume', 'materials_cost', 'works_cost', 'cost']].groupby('ep_id', as_index=False).sum()
    ep = ep.merge(eks_summary, how='left', left_on='id', right_on='ep_id')
    ep['price'] = ep['cost'] / ep['volume']
    ep_list = ep[['id', 'name', 'price', 'volume', 'cost']].to_dict('records')

    ep_list_output = []
    for i, ep_row in enumerate(ep_list):
        logger.debug(f'i: {i}')
        ep_eks = ek.loc[ek.ep_id == ep_row['id']]
        ek_list = ep_eks.drop(columns=['ep_id']).to_dict('records')
        logger.debug(ek_list)
        ek_list_output = []
        # raise Exception
        for j, ek_row in enumerate(ek_list):
            logger.debug(f'j: {j}')
            if ek_row['id'] == 53:
                logger.debug(ek_row)
            ek_base_mat = base_mats.loc[base_mats.ek_id == ek_row['id']]
            ek_base_mat_list = ek_base_mat.drop(columns=['ek_id']).to_dict('records')
            ek_add_mat = add_mats.loc[add_mats.ek_id == ek_row['id']]
            ek_add_mat_list = ek_add_mat.drop(columns=['ek_id']).to_dict('records')
            ek_row['base_mats'] = ek_base_mat_list
            ek_row['add_mats'] = ek_add_mat_list
            ek_row['order_num'] = str(i+1) + '.' + str(j+1)
            ek_list_output.append(ek_row)
            if ek_row['id'] == 53:
                logger.debug(ek_list[j])
        ep_row['eks'] = ek_list_output
        ep_row['order_num'] = str(i+1)
        ep_list_output.append(ep_row)
    
    json_data['eps'] = ep_list_output

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
        **{'work_types_description': '\n'.join(work_types.description.tolist())}
        # **{'contracts_name': f"№ {contracts['number']} от {contracts['date']} {contracts_name}"}
    }
    json_data['props'] = props
    pprint(json_data)
    return json_data


def make_est_materials_table(eng, session, tables, est_id=None, est=None, ek=None, ek_ids=None, objects_id=None):
    if est_id is None and (est is None or ek is None):
        raise Exception('Если не передан id расчета, нужно передать словарь со свойствами расчета и датафрейм с ЕК')
    if est is None or ek is None:
        est = get_table_from_db(session, tables, 'estimations', {'id': est_id})[0]
        ek = get_df_from_db(eng, session, tables, 'ek', {'estimation_id': est_id}, ['id', 'work_types_id', 'volume', 'clc_id'])

    r_work_types_basic_materials = get_df_from_db(eng, session, tables, 'r_work_types_basic_materials', {'work_types_id': ek.work_types_id})
    r_ek_basic_materials = get_df_from_db(eng, session, tables, 'r_ek_basic_materials', {'ek_id': ek.id})
    r_ek_basic_materials['is_basic'] = True
    r_ek_add_materials = get_df_from_db(eng, session, tables, 'r_ek_add_materials', {'ek_id': ek.id})
    r_ek_add_materials['is_basic'] = False
    df = pd.concat([r_ek_basic_materials, r_ek_add_materials], axis=0)
    materials = get_df_from_db(eng, session, tables, 'materials', {'id': df.materials_id}, ['id', 'name', 'ed_izm', 'material_types_id'])
    df = df.merge(materials, how='left', left_on='materials_id', right_on='id', suffixes=[None, '_mat'])
    df = df.merge(ek, how='left', left_on='ek_id', right_on='id', suffixes=[None, '_ek'])
    df = df.merge(r_work_types_basic_materials, how='left', left_on=['work_types_id', 'materials_id'], right_on=['work_types_id', 'materials_id'])
    df['volume'].loc[df['is_basic']] = df['consumption_rate'] * df['volume_ek']
    df.drop(columns=['volume_ek', 'id_ek', 'id_mat', 'work_types_id'], inplace=True)
    
    # Фиксированные цены в СПЦ
    spc_materials_prices = get_df_from_db(eng, session, tables, 'spc_materials_prices', {'spc_id': df.spc_id.dropna()})
    spc = get_df_from_db(eng, session, tables, 'spc', {'id': df.spc_id.dropna()}, remain_cols=['id', 'print_contractor', 'contracts_id'])
    spc_materials_prices = spc_materials_prices.merge(spc, how='left', left_on='spc_id', right_on='id')
    spc_materials_prices.drop(columns=['id'], inplace=True)
    spc_materials_prices['source'] = 'Спецификация'
    contracts = get_df_from_db(eng, session, tables, 'contracts', {'id': spc.contracts_id.dropna()}, ['id', 'contractors_id'])
    spc_materials_prices = spc_materials_prices.merge(contracts, how='left', left_on='contracts_id', right_on='id', suffixes=['_spc', '_contract'])
    spc_materials_prices.drop(columns=['id'], inplace=True)

    
    # Фиксированные цены в КЛК
    clc_materials_prices = get_df_from_db(eng, session, tables, 'clc_materials_prices', {'clc_id': df.clc_id.dropna()})
    # clc = get_df_from_db(eng, session, tables, 'clc', {'id': df.clc_id.dropna()}, remain_cols=['id', 'print_contractor', 'contracts_id'])
    # clc_materials_prices = clc_materials_prices.merge(clc, how='left', left_on='clc_id', right_on='id')
    # clc_materials_prices.drop(columns=['id'], inplace=True)
    clc_materials_prices['source'] = 'Калькуляция'
    prices_history_clc = get_df_from_db(eng, session, tables, 'materials_prices_history', {'id': clc_materials_prices.materials_prices_history_id})
    clc_materials_prices = clc_materials_prices.merge(prices_history_clc, left_on=['materials_prices_history_id', 'materials_id'] , right_on=['id', 'materials_id'])
    clc_materials_prices.drop(columns=['materials_prices_history_id', 'id', 'objects_id', 'date', 'payment_method'], inplace=True)
    logger.debug(clc_materials_prices)
    logger.debug(clc_materials_prices.columns)

    
    # СПЦ + КЛК
    df = df.merge(spc_materials_prices, how='left', on=['spc_id', 'materials_id'])
    df = df.merge(clc_materials_prices, how='left', on=['clc_id', 'materials_id'], suffixes=['_spc', '_clc'])
    df['price'] = df['price_spc'].fillna(df['price_clc'])
    df['source'] = df['source_spc'].fillna(df['source_clc'])
    df['contractors_id'] = df['contractors_id_spc']
    logger.debug(df[['contractors_id']])
    df['contractors_id'].loc[df['spc_id'].isna()] = df['contractors_id_spc'].fillna(df['contractors_id_clc'])
    logger.debug(df[['contractors_id']])
    df.drop(columns=['price_spc', 'price_clc', 'source_clc', 'source_spc', 'contractors_id_spc', 'contractors_id_clc'], inplace=True)

    # Рынок
    market_mats = df.materials_id.loc[df.price.isna()].unique().tolist()
    prices_history_market = tables['materials_prices_history']
    fields = [prices_history_market.c[col] for col in ['id', 'materials_id', 'contractors_id', 'price']]
    prices_history_market = session.query(prices_history_market).with_entities(*fields).filter(
        and_(
            prices_history_market.c['materials_id'].in_(market_mats),
            or_(
                prices_history_market.c['objects_id'] == est['objects_id'],
                prices_history_market.c['objects_id'].is_(None)
            )
        )
    )
    prices_history_market = pd.read_sql(prices_history_market.statement, eng)
    prices_history_market.drop_duplicates(subset=['materials_id'], keep='last', inplace=True)
    logger.debug(prices_history_market)
    prices_history_market['source'] = 'Рынок'
    df = df.merge(prices_history_market, how='left', on='materials_id', suffixes=[None, '_mph'])
    df['price'] = df['price'].fillna(df['price_mph'])
    df['source'] = df['source'].fillna(df['source_mph'])
    df['contractors_id'].loc[df.spc_id.isna()] = df['contractors_id'].fillna(df['contractors_id_mph'])
    df.drop(columns=['price_mph', 'source_mph', 'contractors_id_mph', 'id_mph'], inplace=True)
    
    contractors = get_df_from_db(eng, session, tables, 'contractors', {'id': df.contractors_id.dropna()}, ['id', 'name']).rename(
        columns={'id': 'contractors_id', 'name': 'contractors_name'}
    )
    df = df.merge(contractors, how='left', on='contractors_id')
    df['contractors_name'] = df['contractors_name'].fillna(df['print_contractor'])

    df['cost'] = df.price * df.volume
    df['overconsumption'] = 1
    df = df.replace(np.nan, None)
    return df


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









