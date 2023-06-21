from pyspark.sql import SparkSession

from utils.customize_logger import Logger


class SparkExecutor:
    def __init__(self, spark_session: SparkSession, number_of_workers, logger: Logger):
        self.spark = spark_session
        self.number_of_workers = number_of_workers
        self.logger = logger
