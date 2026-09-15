import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

from thirdsemestrwork.config import DATA_DIR
from thirdsemestrwork.extract import load_networks, load_stations
from thirdsemestrwork.load import create_tables, get_engine, insert_networks, insert_stations

def main() -> None:
    engine = get_engine()
    create_tables(engine)
    insert_networks(engine, load_networks(DATA_DIR))
    insert_stations(engine, load_stations(DATA_DIR))
    engine.dispose()

if __name__ == '__main__':
    main()