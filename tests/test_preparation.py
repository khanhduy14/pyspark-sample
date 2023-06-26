from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, LongType, StructField, FloatType, DoubleType, StringType
import os

os.environ["AWS_ACCESS_KEY_ID"] = "admin"
os.environ["AWS_SECRET_ACCESS_KEY"] = "password"
os.environ["AWS_REGION"] = "us-east-1"


def test_preparation():
    app_name = 'PreparationTest'
    host = 'spark://localhost:7077'

    conf = SparkConf().setAppName(app_name).setMaster(host) \
        .set('spark.jars.packages',
             'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.2.1,software.amazon.awssdk:url-connection-client:2.20.18,software.amazon.awssdk:bundle:2.20.18') \
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
        .set('spark.sql.catalog.rest-demo', 'org.apache.iceberg.spark.SparkCatalog') \
        .set('spark.sql.catalog.rest-demo.catalog-impl', 'org.apache.iceberg.rest.RESTCatalog') \
        .set('spark.sql.catalog.rest-demo.default-namespace', 'rest-demo') \
        .set('spark.sql.catalog.rest-demo.uri', 'http://localhost:8181') \
        .set('spark.sql.catalog.rest-demo.warehouse', 's3://warehouse/') \
        .set('spark.sql.catalog.rest-demo.s3.endpoint', 'http://localhost:9000') \
        .set('spark.sql.catalogImplementation', 'in-memory') \
        .set('spark.sql.defaultCatalog', 'rest-demo') \
        .set('spark.sql.catalog.rest-demo.io-impl', 'org.apache.iceberg.aws.s3.S3FileIO')

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

    # df: DataFrame = Preparation('rest_backend.nyc1.taxis_test', spark, schema, data).get_df()
    df = spark.table('`rest-demo`.nyc.taxis')
    df.show(10, False)
    assert df.count() == 4
    spark.stop()

#x9sW9HUWll1OGy6xT9Nn
#lfaasd8mYF70s9QVk2IOepYPqIXliZWJuJLRrLjZ