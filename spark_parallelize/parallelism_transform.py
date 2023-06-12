from concurrent.futures import ThreadPoolExecutor, as_completed, wait
from typing import List

from pyspark.sql import DataFrame

from spark_executor import SparkExecutor
from utils.transformation_common import TransformationCommon


class ParallelismTransform(SparkExecutor):
    @property
    def _input_transformation(self):
        return List[TransformationCommon]

    def process_input(self) -> List[DataFrame]:
        result = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            fts = {executor.submit(task.transformation): task for task in self._input_transformation()}
            wait(fts)
            for ft in as_completed(fts):
                df = fts[ft]
                try:
                    data = ft.result()
                    result.append(data)
                except Exception as exc:
                    print('generated an exception: %s' % exc)
        return result
