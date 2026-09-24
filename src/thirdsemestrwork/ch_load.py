import logging

import clickhouse_connect

from thirdsemestrwork.config import (
    CH_DATABASE,
    CH_DM_TABLE,
    CH_HOST,
    CH_PASSWORD,
    CH_PORT,
    CH_USER,
    HDFS_DM_PATH,
)

logger = logging.getLogger(__name__)

DM_COLUMNS = """
    network_id String,
    network_name Nullable(String),
    network_latitude Nullable(Float64),
    network_longitude Nullable(Float64),
    city Nullable(String),
    country Nullable(String),
    company Array(String),
    system Nullable(String),
    station_id Nullable(String),
    station_name Nullable(String),
    station_latitude Nullable(Float64),
    station_longitude Nullable(Float64),
    timestamp Nullable(DateTime64(6, 'UTC')),
    free_bikes Nullable(Int32),
    empty_slots Nullable(Int32),
    renting Nullable(Bool),
    returning Nullable(Bool),
    last_updated Nullable(DateTime64(6, 'UTC')),
    slots Nullable(Int32),
    ebikes Nullable(Int32),
    has_ebikes Nullable(Bool),
    payment Array(String),
    payment_terminal Nullable(Bool),
    address Nullable(String),
    post_code Nullable(String),
    network_station String,
    companies_count Nullable(Int32)
"""

HDFS_TABLE = f'{CH_DM_TABLE}_hdfs'

def run_ch_load() -> None:
    client = clickhouse_connect.get_client(
        host=CH_HOST,
        port=CH_PORT,
        username=CH_USER,
        password=CH_PASSWORD,
        database=CH_DATABASE,
    )
    try:
        client.command(f'''
            CREATE OR REPLACE TABLE {HDFS_TABLE} ({DM_COLUMNS})
            ENGINE=HDFS('{HDFS_DM_PATH}/*.parquet', 'Parquet')
        ''')
        client.command(f'''
            CREATE OR REPLACE TABLE {CH_DM_TABLE} ({DM_COLUMNS})
            ENGINE = MergeTree
            ORDER BY network_id
        ''')
        client.command(f'INSERT INTO {CH_DM_TABLE} SELECT * FROM {HDFS_TABLE}')

        count = client.command(f'SELECT count() FROM {CH_DM_TABLE}')
        logger.info('Витрина в ClickHouse %s строк', count)
    finally:
        client.close()
