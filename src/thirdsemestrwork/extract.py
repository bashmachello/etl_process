import json
import logging
from pathlib import Path

logger = logging.getLogger(__name__)
#from thirdsemestrwork.config import DATA_DIR

def load_networks(data_dir: Path) -> list[dict]:
    """Читает networks, возвращает списокй записей"""
    path = data_dir / 'networks.json'
    logger.info('Читаю файл networks: %s', path)
    data = json.loads(path.read_text(encoding='utf-8'))
    networks = data['networks']
    logger.info('networks прочитано: %d', len(networks))
    return networks

def load_stations(data_dir: Path) -> list[dict]:
    """Читает stations, возвращает списокй записей"""
    stations: list[dict] = []
    files = sorted((data_dir / 'stations_output').glob('*.json'))
    logger.info('Файлов в stations: %s', len(files))

    for f in files:
        try:
            data = json.loads(f.read_text(encoding='utf-8'))
        except json.decoder.JSONDecodeError:
            logger.warning('Битый JSON %s', f.name)
            continue


        network_id = f.stem.removeprefix('stations_')
        file_stations = data.get('stations') or []
        if not file_stations:
            logger.info('В network %s нет станций', network_id)
        for station in file_stations:
            row = dict(station)
            row['network_id'] = network_id
            stations.append(row)
    logger.info('stations прочитано: %d', len(stations))
    return stations

#load_stations(DATA_DIR)