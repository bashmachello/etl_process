"""Пайплайн CityBikes: JSON -> Postgres -> Spark витрина на HDFS -> ClickHouse"""

from __future__ import annotations

from datetime import datetime, timedelta

from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import DAG


def _create_tables() -> None:
    from thirdsemestrwork.load import create_tables, get_engine

    engine = get_engine()
    try:
        create_tables(engine)
    finally:
        engine.dispose()

def _load_networks() -> None:
    from thirdsemestrwork.config import DATA_DIR
    from thirdsemestrwork.extract import load_networks
    from thirdsemestrwork.load import get_engine, insert_networks

    engine = get_engine()
    try:
        insert_networks(engine, load_networks(DATA_DIR))
    finally:
        engine.dispose()

def _load_stations() -> None:
    from thirdsemestrwork.config import DATA_DIR
    from thirdsemestrwork.extract import load_stations
    from thirdsemestrwork.load import get_engine, insert_stations

    engine = get_engine()
    try:
        insert_stations(engine, load_stations(DATA_DIR))
    finally:
        engine.dispose()

def _build_datamart() -> None:
    from thirdsemestrwork.datamart import run_datamart

    run_datamart()

def _load_to_clickhouse() -> None:
    from thirdsemestrwork.ch_load import run_ch_load

    run_ch_load()

def _check_counts() -> None:
    import logging

    import clickhouse_connect
    from sqlalchemy import text

    from thirdsemestrwork.config import (
        CH_DATABASE,
        CH_DM_TABLE,
        CH_HOST,
        CH_PASSWORD,
        CH_PORT,
        CH_USER,
        NETWORKS_TABLE,
        STATIONS_TABLE,
    )
    from thirdsemestrwork.load import get_engine

    log = logging.getLogger(__name__)

    engine = get_engine()
    try:
        with engine.connect() as conn:
            def q(sql: str):
                return conn.execute(text(sql)).scalar_one()
            pg_networks = q(f'SELECT count(*) FROM {NETWORKS_TABLE}')
            pg_stations = q(f'SELECT count(*) FROM {STATIONS_TABLE}')
            pg_stations_no_net  = q(f'''SELECT count(*) FROM {STATIONS_TABLE} s
                LEFT JOIN {NETWORKS_TABLE} n ON s.network_id = n.id
                WHERE n.id IS NULL''')
            pg_renting = q(f'''SELECT count(*) FROM {STATIONS_TABLE}
                WHERE extra->>'renting' IS NOT NULL''')
            pg_renting_no_net = q(f'''SELECT count(*) FROM {STATIONS_TABLE} s
                LEFT JOIN {NETWORKS_TABLE} n ON s.network_id = n.id
                WHERE n.id IS NULL AND s.extra->>'renting' IS NOT NULL''')
            pg_ts = q(f'''SELECT count(*) FROM {STATIONS_TABLE}
                WHERE "timestamp" IS NOT NULL''')
            pg_ts_no_net = q(f'''SELECT count(*) FROM {STATIONS_TABLE} s
                LEFT JOIN {NETWORKS_TABLE} n ON s.network_id = n.id
                WHERE n.id IS NULL AND s."timestamp" IS NOT NULL''')
    finally:
        engine.dispose()

    ch = clickhouse_connect.get_client(
        host=CH_HOST, port=CH_PORT, username=CH_USER,
        password=CH_PASSWORD, database=CH_DATABASE
    )
    try:
        def c(sql: str):
            return ch.command(sql)
        ch_total = c(f'SELECT count() FROM {CH_DM_TABLE}')
        ch_stations = c(f'SELECT count(station_id) FROM {CH_DM_TABLE}')
        ch_networks = c(f'SELECT uniqExact(network_id) FROM {CH_DM_TABLE}')
        hdfs_rows = c(f'SELECT count() FROM {CH_DM_TABLE}_hdfs')
        ch_renting = c(f'SELECT countIf(isNotNull(renting)) FROM {CH_DM_TABLE}')
        ch_ts = c(f'SELECT countIf(isNotNull(timestamp)) FROM {CH_DM_TABLE}')
    finally:
        ch.close()

    problems = []
    if ch_networks != pg_networks:
        problems.append(f'networks: in Postgres {pg_networks}, in datamart {ch_networks}')
    if ch_stations != pg_stations - pg_stations_no_net:
        problems.append(
            f'stations: in Postgres {pg_stations} '
            f'({pg_stations_no_net} without rec about stations), in datamart {ch_stations}')
    if hdfs_rows != ch_total:
        problems.append(f'rows: in HDFS {hdfs_rows}, in ClickHouse {ch_total}')
    if ch_renting != pg_renting - pg_renting_no_net:
        problems.append(
            f'renting заполнено: в Postgres {pg_renting - pg_renting_no_net}, '
            f'в витрине {ch_renting}')
    if ch_ts != pg_ts - pg_ts_no_net:
        problems.append(
            f'timestamp заполнено: в Postgres {pg_ts - pg_ts_no_net}, '
            f'в витрине {ch_ts}')

    if problems:
        raise RuntimeError('Check false: ' + '; '.join(problems))

    log.info('Check complete: networks %d, stations %d '
             '(in Postgres %d, inc %d without records about network), '
             'renting filled %d, timestamp filled %d, rows in datamart %d',
             ch_networks, ch_stations, pg_stations, pg_stations_no_net,
             ch_renting, ch_ts, ch_total)

with DAG(
    dag_id='citybikes_load',
    start_date=datetime(2026, 9, 15),
    schedule=None,
    catchup=False,
    default_args={'retries': 2, 'retry_delay': timedelta(minutes=1)},
    tags=['citybikes', 'first', 'load', 'spiridonov'],
    doc_md=__doc__,
) as dag:
    create = PythonOperator(task_id='create_tables', python_callable=_create_tables)
    networks = PythonOperator(task_id='load_networks', python_callable=_load_networks)
    stations = PythonOperator(task_id='load_stations', python_callable=_load_stations)
    mart = PythonOperator(task_id='mart', python_callable=_build_datamart)
    ch = PythonOperator(task_id='ch', python_callable=_load_to_clickhouse)
    check = PythonOperator(task_id='check_counts', python_callable=_check_counts)

    create >> [networks, stations]
    [networks, stations] >> mart
    mart >> ch
    ch >> check
