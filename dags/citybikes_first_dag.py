from __future__ import annotations

from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

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

with DAG(
    dag_id='citybikes_load',
    start_date=datetime(2026, 9, 15),
    schedule=None,
    catchup=False,
    tags=['citybikes', 'first', 'load', 'spiridonov'],
    doc_md=__doc__,
) as dag:
    create = PythonOperator(task_id='create_tables', python_callable=_create_tables)
    networks = PythonOperator(task_id='load_networks', python_callable=_load_networks)
    stations = PythonOperator(task_id='load_stations', python_callable=_load_stations)

    create >> [networks, stations]