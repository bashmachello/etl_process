import json
from pathlib import Path

from thirdsemestrwork.extract import load_networks, load_stations
from thirdsemestrwork.load import _station_id

def test_station_id_stable_and_unique():
    st = {'name': 'A', 'latitude': 1.0, 'longitude': 2.0}
    first = _station_id('net', st)
    assert first == _station_id('net', st)
    assert len(first) == 32 and int(first, 16) >= 0
    assert first != _station_id('other_net', st)
    assert first != _station_id('net', {**st, 'name': 'B'})

def test_load_networks(tmp_path):
    (tmp_path / 'networks.json').write_text(
        json.dumps({'networks': [{'id': 'x'}]}), encoding='utf-8')
    assert load_networks(tmp_path) == [{'id': 'x'}]

def test_load_stations(tmp_path):
    d = tmp_path / 'stations_output'
    d.mkdir()
    (d / 'stations_alpha.json').write_text(
        json.dumps({'stations': [{'name': 'A', 'bikes': 1}]}), encoding='utf-8')
    rows = load_stations(tmp_path)
    assert len(rows) == 1
    assert rows[0]['network_id'] == 'alpha'

def test_load_stations_skip_broken_json(tmp_path):
    d = tmp_path / 'stations_output'
    d.mkdir()
    (d / 'stations_good.json').write_text('{"stations": [{"name": "A"}]}', encoding='utf-8')
    (d / 'stations_broken.json').write_text('broken', encoding='utf-8')
    rows = load_stations(tmp_path)
    assert len(rows) == 1

def test_load_stations_empty_and_missing(tmp_path):
    d = tmp_path / 'stations_output'
    d.mkdir()
    (d / 'stations_empty.json').write_text('{"stations": []}', encoding='utf-8')
    (d / 'stations_no_key.json').write_text('{}', encoding='utf-8')
    assert load_stations(tmp_path) == []