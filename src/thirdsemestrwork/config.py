import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / 'data'

DATABASE_URL = os.getenv('DATABASE_URL',
                         'postgresql+psycopg2://etl:etl@localhost:5433/citybikes')

NETWORKS_TABLE = 'a_spiridonov_networks'
STATIONS_TABLE = 'a_spiridonov_stations'

CH_HOST = os.getenv('CH_HOST', 'clickhouse')
CH_PORT = int(os.getenv('CH_PORT', '8123'))
CH_USER = os.getenv('CH_USER', 'etl')
CH_PASSWORD = os.getenv('CH_PASSWORD', 'etl')
CH_DATABASE = os.getenv('CH_DATABASE', 'citybikes')
CH_DM_TABLE = os.getenv('CH_DM_TABLE', 'a_spiridonov_dm')

HDFS_DM_PATH = os.getenv('HDFS_DM_PATH', 'hdfs://namenode:8020/user/a_spiridonov/dm')
