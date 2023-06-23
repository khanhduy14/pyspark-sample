from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, LongType, StructField, FloatType, DoubleType, StringType

from tests.preparation import Preparation


def test_preparation():
    app_name = 'PreparationTest'
    host = 'spark://localhost:7077'
    conf = SparkConf().setAppName(app_name).setMaster(host) \
        .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.2.1') \
        .set('spark.sql.catalog.rest_prod', 'org.apache.iceberg.spark.SparkCatalog') \
        .set('spark.sql.catalog.rest_prod.type', 'rest') \
        .set('spark.sql.catalog.rest-prod.default-namespace', 'rest') \
        .set('spark.sql.catalog.rest_prod.uri', 'http://localhost:8181')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    schema = StructType([
        StructField("vendor_id", LongType(), True),
        StructField("trip_id", LongType(), True),
        StructField("trip_distance", FloatType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("store_and_fwd_flag", StringType(), True)
    ])

    data = [
        (1, 1000371, 1.8, 15.32, "N"),
        (2, 1000372, 2.5, 22.15, "N"),
        (2, 1000373, 0.9, 9.01, "N"),
        (1, 1000374, 8.4, 42.13, "Y")
    ]

    df: DataFrame = Preparation('rest.nyc.taxis_test', spark, schema, data).get_df()
    assert df.count() == 4
    spark.stop()
