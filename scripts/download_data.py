import json
import logging
import time
from pathlib import Path

import requests

BASE_URL = 'https://api.citybik.es/v2/'
DATA_DIR = Path(__file__).resolve().parent.parent / 'data'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log = logging.getLogger(__name__)

def main() -> None:
    stations_dir = DATA_DIR / 'stations_output'
    stations_dir.mkdir(parents=True, exist_ok=True)

    log.info('Скачивание данных')
    networks = requests.get(f'{BASE_URL}/networks', timeout=30).json()
    (DATA_DIR / 'networks.json').write_text(json.dumps(networks), encoding='utf-8')
    log.info('Сетей: %d', len(networks['networks']))

    for i, net in enumerate(networks['networks'], 1):
        net_id = net['id']
        out_file = stations_dir / f'{net_id}.json'
        if out_file.exists():
            continue
        resp = requests.get(f'{BASE_URL}/networks/{net_id}', timeout=30)
        if resp.status_code != 200:
            log.warning('Пропуск %s: HTTP %s', net_id, resp.status_code)
            continue
        out_file.write_text(resp.text, encoding='utf-8')
        log.info('[%d] ok %s', i, net_id)
        time.sleep(1)

if __name__ == '__main__':
    main()