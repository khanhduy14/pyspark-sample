from pyspark.sql import SparkSession


class SparkExecutor:
    def __init__(self, spark_session: SparkSession):
        self.spark = spark_session

