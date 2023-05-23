from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, LongType, StructField, FloatType, DoubleType, StringType

from tests.preparation import Preparation


def test_preparation():
    app_name = 'PreparationTest'
    host = 'spark://localhost:7077'
    # spark.jars.packages                                  org.apache.iceberg:iceberg-spark-runtime-3.2_2.12:1.2.1
    # spark.sql.extensions                                 org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions
    # spark.sql.catalog.spark_catalog                      org.apache.iceberg.spark.SparkSessionCatalog
    # spark.sql.catalog.spark_catalog.type                 hive
    # spark.sql.catalog.local                              org.apache.iceberg.spark.SparkCatalog
    # spark.sql.catalog.local.type                         hadoop
    # spark.sql.catalog.local.warehouse                    $PWD/warehouse
    # spark.sql.defaultCatalog                             local
    conf = SparkConf().setAppName(app_name).setMaster(host) \
        .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.2.1') \
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
        .set('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog') \
        .set('spark.sql.catalog.spark_catalog.type', 'hive') \
        .set('spark.sql.catalog.local', 'org.apache.iceberg.spark.SparkCatalog') \
        .set('spark.sql.catalog.local.type', 'hadoop') \
        .set('spark.sql.defaultCatalog', 'local') \
        .set('spark.sql.catalog.local.warehouse', '/Users/duykk/Desktop/SideProjects/docker-spark-iceberg/warehouse')
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

    df: DataFrame = Preparation('demo.nyc.taxis_test', spark, schema, data).get_df()
    assert df.count() == 4
