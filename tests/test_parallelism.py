from typing import List

from pyspark import SparkConf
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import lit

from spark_parallelize.parallelism_transform import ParallelismTransform
from utils.transformation_common import TransformationCommon


class TransformationTest1(TransformationCommon):
    def transformation(self):
        self.df = self.df.withColumn('test1', lit('test1'))
        return self.df


class TransformationTest2(TransformationCommon):
    def transformation(self):
        self.df = self.df.withColumn('test1', lit('test2'))
        return self.df


class TestParallelism(ParallelismTransform):
    def __init__(self, spark_session: SparkSession, number_of_workers, input: List[TransformationCommon]):
        super().__init__(spark_session, number_of_workers)
        self.input = input

    def _input_transformation(self):
        return [df for df in self.input]


def test_parallelism():
    app_name = 'Test Parallelism'
    host = 'spark://localhost:7077'
    conf = SparkConf().setAppName(app_name).setMaster(host).set("spark.driver.host", "localhost")\
        .set("spark.driver.bindAddress", "localhost") \
        .set('spark.jars.packages', 'org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.2.1') \
        .set('spark.sql.extensions', 'org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions') \
        .set('spark.sql.catalog.spark_catalog', 'org.apache.iceberg.spark.SparkSessionCatalog') \
        .set('spark.sql.catalog.spark_catalog.type', 'hive') \
        .set('spark.sql.catalog.local', 'org.apache.iceberg.spark.SparkCatalog') \
        .set('spark.sql.catalog.local.type', 'hadoop') \
        .set('spark.sql.defaultCatalog', 'local') \
        .set('spark.sql.catalog.local.warehouse', '/Users/duykk/Desktop/SideProjects/docker-spark-iceberg/warehouse')
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    df = df2 = spark.table('demo.nyc.taxis')
    tf = TransformationTest1(df)
    tf2 = TransformationTest2(df2)

    outs = TestParallelism(spark, 10, [tf, tf2]).process_input()
    final = outs[0].union(outs[1])
    assert len(outs) == 2
    spark.stop()
