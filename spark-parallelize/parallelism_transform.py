from concurrent.futures import ThreadPoolExecutor, wait, as_completed
from typing import List

from pyspark.sql import SparkSession

from spark_executor import SparkExecutor
from utils.transformation_common import TransformationCommon


class ParallelismTransform(SparkExecutor):
    def __init__(self, spark_session: SparkSession, number_of_worker):
        super().__init__(spark_session)
        self.number_of_worker = number_of_worker

    @property
    def _input_transformation(self):
        return List[TransformationCommon]

    def process_input(self):
        with ThreadPoolExecutor(max_workers=10) as executor:
            fts = []
            for task in self._input_transformation():
                fts.append(executor.submit(task.transformation))

            wait(fts)
            for ft in as_completed(fts):
                input = ft