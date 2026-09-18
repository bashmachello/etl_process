import os
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
DATA_DIR = PROJECT_ROOT / 'data'

DATABASE_URL = os.getenv('DATABASE_URL',
                         'postgresql+psycopg2://etl:etl@localhost:5433/citybikes')

NETWORKS_TABLE = 'a_spiridonov_networks'
STATIONS_TABLE = 'a_spiridonov_stations'

HDFS_DM_PATH = 'hdfs://namenode:8020/user/a_spiridonov/dm'

CH_HOST = 'clickhouse'
CH_PORT = 8123
CH_USER = 'etl'
CH_PASSWORD = 'etl'
CH_DATABASE = 'citybikes'
CH_DM_TABLE = 'a_spiridonov_dm'