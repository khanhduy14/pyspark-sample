import unittest

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, LongType, StructField, FloatType, DoubleType, StringType

from tests.preparation import Preparation


class PreparationTest(unittest.TestCase):
    def __init__(self):
        super().__init__()
        app_name = 'PreparationTest'
        host = 'spark://localhost:7077'
        conf = SparkConf().setAppName(app_name).setMaster(host)
        self.spark = SparkSession.builder.config(conf=conf).getOrCreate()

    def test_preparation(self):
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

        df: DataFrame = Preparation('demo.nyc.taxis', self.spark, schema, data).get_df()
        self.assertEqual(df.count(), 4)  # add assertion here


if __name__ == '__main__':
    unittest.main()
