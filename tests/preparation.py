from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType


class Preparation:
    def __init__(self, table_name: str, spark: SparkSession, schema: StructType, data):
        self.data = data
        self.table_name = table_name
        self.spark = spark
        self.schema = schema
        self.create_table()
        self.write_data()

    def create_table(self):
        df = self.spark.createDataFrame([], self.schema)
        df.writeTo(self.table_name).createOrReplace()
        df.explain(mode="formatted")

    def write_data(self):
        df = self.spark.createDataFrame(self.data, self.schema)
        df.writeTo(self.table_name).append()
        df.explain(mode="formatted")


    def get_df(self) -> DataFrame:
        df = self.spark.table(self.table_name)
        df.explain(mode='formatted')
        return df
