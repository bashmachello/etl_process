import logging

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

from thirdsemestrwork.config import (
    DATABASE_URL,
    HDFS_DM_PATH,
    NETWORKS_TABLE,
    STATIONS_TABLE
)

logger = logging.getLogger(__name__)

LOCATION_SCHEMA = StructType([
    StructField('latitude', DoubleType()),
    StructField('longitude', DoubleType()),
    StructField('city', StringType()),
    StructField('country', StringType()),
])

COMPANY_SCHEMA = ArrayType(StringType())

EXTRA_SCHEMA = StructType([
    StructField('renting', StringType()),
    StructField('returning', StringType()),
    StructField('last_updated', StringType()),
    StructField('slots', IntegerType()),
    StructField('ebikes', IntegerType()),
    StructField('has_ebikes', BooleanType()),
    StructField('payment', ArrayType(StringType())),
    StructField('payment-terminal', BooleanType()),
    StructField('address', StringType()),
    StructField('post_code', StringType()),
])

def _jdbc_settings(url: str) -> dict:
    rest = url.split('://', 1)[1]
    creds, host_db = rest.rsplit('@', 1)
    user, password = creds.split(':', 1)
    return {
        'url': f'jdbc:postgresql://{host_db}',
        'user': user,
        'password': password,
        'driver': 'org.postgresql.Driver',
    }

def _to_bool(col):
    v = F.lower(F.trim(col))
    return (
        F.when(v.isin('1', 'true'), F.lit(True))
        .when(v.isin('0', 'false'), F.lit(False))
    )

def _parse_datetime(col):
    as_double = col.try_cast('double')
    return F.coalesce(
        F.try_to_timestamp(col),
        F.when(F.length(col) >= 11, (as_double / 1000).cast('timestamp'))
        .otherwise(as_double.cast('timestamp'))
    )

def build_datamart(spark: SparkSession) -> None:
    jdbc = _jdbc_settings(DATABASE_URL)

    nets = spark.read.format('jdbc').options(
        **jdbc,
        dbtable=f'(SELECT id, name, location::text AS location, '
                f'company::text AS company, system FROM {NETWORKS_TABLE}) nets'
    ).load()

    sts = spark.read.format('jdbc').options(
        **jdbc,
        dbtable=f'(SELECT network_id, station_id, name, latitude, longitude, '
                f'"timestamp", bikes, free, extra::text AS extra FROM {STATIONS_TABLE}) sts',
    ).load()

    nets_parsed = nets.select(
        F.col('id'),
        F.col('name').alias('network_name'),
        F.from_json(F.col('location'), LOCATION_SCHEMA).alias('loc'),
        F.from_json(F.col('company'), COMPANY_SCHEMA).alias('company'),
        F.col('system'),
    ).alias('n')

    sts_parsed = sts.select(
        F.col('network_id'),
        F.col('station_id'),
        F.col('name').alias('station_name'),
        F.col('latitude').alias('station_latitude'),
        F.col('longitude').alias('station_longitude'),
        F.col('timestamp'),
        F.col('bikes'),
        F.col('free'),
        F.from_json(F.col('extra'), EXTRA_SCHEMA).alias('ex'),
    ).alias('s')

    dm = (
        sts_parsed.join(nets_parsed, F.col('s.network_id') == F.col('n.id'))
        .select(
            F.col('n.id').alias('network_id'),
            F.trim(F.col('n.network_name')).alias('network_name'),
            F.col('n.loc.latitude').alias('network_latitude'),
            F.col('n.loc.longitude').alias('network_longitude'),
            F.trim(F.col('n.loc.city')).alias('city'),
            F.trim(F.col('n.loc.country')).alias('country'),
            F.transform(F.col('n.company'), lambda x: F.trim(x)).alias('company'),
            F.trim(F.col('n.system')).alias('system'),
            F.col('s.station_id'),
            F.trim(F.col('s.station_name')).alias('station_name'),
            F.col('s.station_latitude'),
            F.col('s.station_longitude'),
            _parse_datetime(F.col('s.timestamp')).alias('timestamp'),
            F.col('s.bikes').cast('int').alias('free_bikes'),
            F.col('s.free').cast('int').alias('empty_slots'),
            _to_bool(F.col('s.ex.renting')).alias('renting'),
            _to_bool(F.col('s.ex.returning')).alias('returning'),
            _parse_datetime(F.col('s.ex.last_updated')).alias('last_updated'),
            F.col('s.ex.slots').cast('int').alias('slots'),
            F.col('s.ex.ebikes').cast('int').alias('ebikes'),
            F.col('s.ex.has_ebikes'),
            F.col('s.ex.payment'),
            F.col('s.ex').getField('payment-terminal').alias('payment_terminal'),
            F.trim(F.col('s.ex.address')).alias('address'),
            F.trim(F.col('s.ex.post_code')).alias('post_code'),
            F.concat_ws('/', F.trim(F.col('n.network_name')),
                        F.trim(F.col('s.station_name'))).alias('network_station'),
            F.when(F.col('n.company').isNotNull(), F.size(F.col('n.company')))
            .otherwise(F.lit(0))
            .cast('int')
            .alias('companies_count'),
        )
    )

    total = dm.count()
    logger.info('Витрина собрана %d строк', total)
    dm.printSchema()

    dm.write.mode('overwrite').parquet(HDFS_DM_PATH)
    logger.info('Витрина записана %s', HDFS_DM_PATH)

def run_datamart() -> None:
    spark = (
        SparkSession.builder
        .appName('citybikes_datamart')
        .master('local[2]')
        .config('spark.driver.memory', '2g')
        .config('spark.sql.shuffle.partitions', '4')
        .config('spark.jars.packages', 'org.postgresql:postgresql:42.7.4')
        .config('spark.ui.enabled', 'false')
        .getOrCreate()
    )
    try:
        build_datamart(spark)
    finally:
        spark.stop()


