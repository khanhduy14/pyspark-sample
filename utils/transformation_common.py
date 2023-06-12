from pyspark.sql import DataFrame


class TransformationCommon:
    def __init__(self, df: DataFrame):
        self.df = df

    @property
    def transformation(self):
        return DataFrame
