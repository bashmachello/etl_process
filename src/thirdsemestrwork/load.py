import logging
import hashlib

from sqlalchemy import Boolean, Column, Double, Integer, MetaData, String, Table, Text, create_engine
from sqlalchemy.dialects.postgresql import JSONB, insert

from thirdsemestrwork.config import DATABASE_URL, NETWORKS_TABLE, STATIONS_TABLE

logger = logging.getLogger(__name__)

metadata = MetaData()

networks_table = Table(
    NETWORKS_TABLE,
    metadata,
Column('id', String, primary_key=True),
    Column('name', Text),
    Column('location', JSONB(none_as_null=True)),
    Column('href', Text),
    Column('company', JSONB(none_as_null=True)),
    Column('system', Text),
    Column('gbfs_href', Text),
    Column('source', Text),
    Column('license', JSONB(none_as_null=True)),
    Column('ebikes', Boolean),
    Column('scooters', Boolean),
    Column('instances', JSONB(none_as_null=True)),
)

stations_table = Table(
    STATIONS_TABLE,
    metadata,
Column('network_id', String, primary_key=True),
    Column('station_id', String, primary_key=True),
    Column('name', Text),
    Column('latitude', Double),
    Column('longitude', Double),
    Column('timestamp', Text),
    Column('bikes', Integer),
    Column('free', Integer),
    Column('extra', JSONB(none_as_null=True)),
)

NETWORK_FIELDS = ['id', 'name', 'location', 'href', 'company', 'system', 'gbfs_href', 'source', 'license', 'ebikes', 'scooters', 'instances']


def create_tables(engine) -> None:
    """Создаем таблицы если их нет"""
    metadata.create_all(engine)
    logger.info('Таблицы созданы: %s, %s', NETWORK_FIELDS, STATIONS_TABLE)

def insert_networks(engine, networks: list[dict]) -> None:
    rows = []
    skipped = 0
    for net in networks:
        if net.get('id') is None:
            skipped += 1
            continue
        rows.append({f: net.get(f) for f in NETWORK_FIELDS})
    if skipped:
        logger.warning('Пропущено networks без id: %d', skipped)

    with engine.begin() as conn:
        for i in range(0, len(rows), 500):
            stmt = insert(networks_table).values(rows[i:i + 500])
            conn.execute(stmt.on_conflict_do_nothing(index_elements=['id']))
    logger.info('networks записано %d', len(rows))


def _station_id(network_id: str, station: dict) -> str:
    raw = f"{network_id}|{station.get('name')}|{station.get('latitude')}|{station.get('longitude')}"
    return hashlib.md5(raw.encode('utf-8')).hexdigest()

def insert_stations(engine, stations: list[dict]) -> None:
    rows = [{
        'network_id': st['network_id'],
        'station_id': _station_id(st['network_id'], st),
        'name': st.get('name'),
        'latitude': st.get('latitude'),
        'longitude': st.get('longitude'),
        'timestamp': st.get('timestamp'),
        'bikes': st.get('bikes'),
        'free': st.get('free'),
        'extra': st.get('extra'),
        } for st in stations]

    with engine.begin() as conn:
        for i in range(0, len(rows), 1000):
            stmt = insert(stations_table).values(rows[i:i + 1000])
            conn.execute(stmt.on_conflict_do_nothing(index_elements=['network_id', 'station_id']))
    logger.info('stations записано %d', len(rows))

def get_engine():
    return create_engine(DATABASE_URL)

