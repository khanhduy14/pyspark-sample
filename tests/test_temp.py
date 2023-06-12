from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, LongType, StructField, FloatType, DoubleType, StringType


def test_():
    conf = SparkConf().setMaster('spark://localhost:7077').setAppName("test")
    spark = SparkSession.builder.config(conf=conf).getOrCreate()


