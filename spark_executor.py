from pyspark.sql import SparkSession


class SparkExecutor:
    def __init__(self, spark_session: SparkSession, number_of_workers):
        self.spark = spark_session
        self.number_of_workers = number_of_workers

