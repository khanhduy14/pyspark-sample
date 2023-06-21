import sys
import traceback
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
                try:
                    df = fts[ft]
                    data = ft.result()
                    result.append(data)
                except Exception as exc:
                    print('generated an exception: %s' % exc)
                    print(f"Exception: {str(exc)}")
                    ex_type, ex_value, ex_traceback = sys.exc_info()
                    # Extract unformatter stack traces as tuples
                    trace_back = traceback.extract_tb(ex_traceback)
                    # Format stacktrace
                    stack_trace = list()
                    stack_trace.append("Exception type : %s " % ex_type.__name__)
                    stack_trace.append("Exception message : %s" % ex_value)
                    for trace in trace_back:
                        stack_trace.append("File : %s , Line : %d, Func.Name : %s, Message : %s" % (
                        trace[0], trace[1], trace[2], trace[3]))
                    self.logger.error('\n'.join(stack_trace))
                    has_error = True
                else:
                    self.logger.info(f'Finish DS_SOURCE: {df} finised with result: {data}')
        return result
