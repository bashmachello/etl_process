import json
from collections import Counter
from pathlib import Path

DATA_DIR = Path(__file__).resolve().parent.parent / 'data'

def analyze(records: list, label: str) -> None:
    '''Считает в скольких записях есть и какой тип данных'''

    total = len(records)
    field_counts: Counter = Counter()
    field_types: dict[str, set] = {}
    for rec in records:
        for key, value in rec.items():
            field_counts[key] += 1
            field_types.setdefault(key, set()).add(type(value).__name__)

    print(f'{label}: {total} записей')
    for name, count in field_counts.most_common():
        pct = count / total * 100 if total else 0
        types = ', '.join(sorted(field_types[name]))
        print(f'{name} {count} / {total} ({pct}%), типы {types}')

def main() -> None:
    networks = json.loads((DATA_DIR / 'networks.json').read_text(encoding='utf-8'))
    analyze(networks['networks'], 'networks')

    stations: list = []
    files = sorted((DATA_DIR / 'stations_output').glob('*.json'))
    for f in files:
        try:
            data = json.loads(f.read_text(encoding='utf-8'))
        except json.JSONDecodeError:
            print(f'Битый файл: {f.name}')
            continue
        stations.extend(data.get('stations', []))
    analyze(stations, f'stations ({len(files)}) файлов')


if __name__ == '__main__':
    main()