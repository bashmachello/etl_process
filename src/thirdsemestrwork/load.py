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
    logger.info('Таблицы созданы: %s, %s', NETWORKS_TABLE, STATIONS_TABLE)

def insert_networks(engine, networks: list[dict]) -> None:
    rows_map: dict = {}
    skipped = 0
    for net in networks:
        if net.get('id') is None:
            skipped += 1
            continue
        rows_map.setdefault(net['id'], {f: net.get(f) for f in NETWORK_FIELDS})
    rows = list(rows_map.values())

    if skipped:
        logger.warning('Пропущено networks без id: %d', skipped)

    inserted = 0
    with engine.begin() as conn:
        for i in range(0, len(rows), 500):
            stmt = insert(networks_table).values(rows[i:i + 500])
            result = conn.execute(stmt.on_conflict_do_update(
                index_elements=['id'],
                set_={
                    'name': stmt.excluded.name,
                    'location': stmt.excluded.location,
                    'href': stmt.excluded.href,
                    'company': stmt.excluded.company,
                    'system': stmt.excluded.system,
                    'gbfs_href': stmt.excluded.gbfs_href,
                    'source': stmt.excluded.source,
                    'license': stmt.excluded.license,
                    'ebikes': stmt.excluded.ebikes,
                    'scooters': stmt.excluded.scooters,
                    'instances': stmt.excluded.instances,
                },
            ))
            inserted += result.rowcount
    logger.info('networks: вставлено/обновлено %d из %d', inserted, len(rows))


def _station_id(network_id: str, station: dict) -> str:
    raw = f"{network_id}|{station.get('name')}|{station.get('latitude')}|{station.get('longitude')}"
    return hashlib.md5(raw.encode('utf-8')).hexdigest()

def insert_stations(engine, stations: list[dict]) -> None:
    rows_map: dict = {}
    for st in stations:
        station_id = _station_id(st['network_id'], st)
        key = (st['network_id'], station_id)
        if key not in rows_map:
            rows_map[key] = {
                'network_id': st['network_id'],
                'station_id': station_id,
                'name': st.get('name'),
                'latitude': st.get('latitude'),
                'longitude': st.get('longitude'),
                'timestamp': st.get('timestamp'),
                'bikes': st.get('bikes'),
                'free': st.get('free'),
                'extra': st.get('extra'),
                }
    rows = list(rows_map.values())

    inserted = 0
    with engine.begin() as conn:
        for i in range(0, len(rows), 1000):
            stmt = insert(stations_table).values(rows[i:i + 1000])
            result = conn.execute(stmt.on_conflict_do_update(
                index_elements=['network_id', 'station_id'],
                set_={
                    'timestamp': stmt.excluded.timestamp,
                    'bikes': stmt.excluded.bikes,
                    'free': stmt.excluded.free,
                    'extra': stmt.excluded.extra,
                },
            ))
            inserted += result.rowcount
    logger.info('stations: вставлено/обновлено %d из %d', inserted, len(rows))

def get_engine():
    return create_engine(DATABASE_URL)

