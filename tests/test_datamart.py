import os
import sys

import pytest

pytest.importorskip('pyspark', reason='нет pyspark локально: uv sync --group pipeline')

os.environ.setdefault('PYSPARK_PYTHON', sys.executable)

from pyspark.sql import SparkSession  # noqa: E402
from pyspark.sql import functions as F

from thirdsemestrwork.datamart import _parse_datetime, _to_bool, _jdbc_settings  # noqa: E402


@pytest.fixture(scope='module')
def spark():
    try:
        session = (
            SparkSession.builder
            .master('local[1]')
            .appName('tests')
            .config('spark.ui.enabled', 'false')
            .config('spark.sql.session.timeZone', 'UTC')
            .getOrCreate()
        )
    except Exception as e:
        pytest.skip(f'Spark не стартовал (нет Java локально?): {e}')
    yield session
    session.stop()


def one_bool(spark, value):
    df = spark.createDataFrame([(value,)], 'v string')
    return df.select(_to_bool(F.col('v')).alias('r')).collect()[0]['r']


def one_ts(spark, value):
    df = spark.createDataFrame([(value,)], 'v string')
    return df.select(
        F.date_format(_parse_datetime(F.col('v')), 'yyyy-MM-dd HH:mm:ss.SSSSSS').alias('r')
    ).collect()[0]['r']


@pytest.mark.parametrize('value, expected', [
    ('1', True),
    ('true', True),
    ('TRUE', True),
    (' 1 ', True),
    ('0', False),
    ('false', False),
    ('False', False),
    ('мусор', None),
    (None, None),
])
def test_to_bool(spark, value, expected):
    assert one_bool(spark, value) == expected


@pytest.mark.parametrize('value, expected', [
    ('2025-05-12 13:30:36.736960+00:00', '2025-05-12 13:30:36.736960'),
    ('2025-05-12T15:54:20+02:00', '2025-05-12 13:54:20.000000'),
    ('1747056454', '2025-05-12 13:27:34.000000'),
    ('1747056454000', '2025-05-12 13:27:34.000000'),
    ('2026-09-21T09:28:33.390631+00:00Z', '2026-09-21 09:28:33.390631'),
    ('15/04/2025 10:00:00', '2025-04-15 10:00:00.000000'),
    ('это не дата', None),
    (None, None),
])
def test_parse_datetime(spark, value, expected):
    assert one_ts(spark, value) == expected

def test_jdbc_settings_decodes_percent_encoding():
    s = _jdbc_settings('postgresql+psycopg2://etl:p%40ss%2Fword@postgres:5432/citybikes')
    assert s['user'] == 'etl'
    assert s['password'] == 'p@ss/word'
    assert s['url'] == 'jdbc:postgresql://postgres:5432/citybikes'