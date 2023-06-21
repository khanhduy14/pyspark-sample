import datetime
import logging
import os
import sys

NUMBER_ROW_TO_SHOW = 10
NUMBER_CHAR_TO_SHOW = 50


class Logger(logging.getLoggerClass()):
    def __init__(self, name, log_level: logging.INFO, log_dir=None):
        # Create custom logger logging all five levels
        super().__init__(name)

        self.setLevel(log_level)

        # Set up logging format.
        _FORMAT_TEXT = "[%(asctime)s][%(levelname)s] %(filename)s:%(lineno)d:\t%(message)s"
        _FORMAT_TIME = "%m/%d %H:%M:%S"

        formatter = logging.Formatter(
            fmt=_FORMAT_TEXT,
            datefmt=_FORMAT_TIME,
        )

        # Create stream handler for logging to stdout (log all five levels)
        self.stdout_handler = logging.StreamHandler(sys.stdout)
        self.stdout_handler.setLevel(logging.DEBUG)
        self.stdout_handler.setFormatter(formatter)
        self.enable_console_output()

        self.file_handler = None
        if log_dir:
            self.add_file_handler(name, log_dir)

        self.debug_mode = None

    def add_file_handler(self, name, log_dir):
        """Add a file handler for this logger with the specified `name` (and store the log file
        under `log_dir`)."""
        # Format for file log
        fmt = "%(asctime)s | %(levelname)9s | %(filename)s:%(lineno)d | %(message)s"
        formatter = logging.Formatter(fmt)

        # Determine log path and file name; create log path if it does not exist
        now = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_name = f'{str(name).replace(" ", "_")}_{now}'
        if not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir)
            except Exception as e:
                # fmt: off
                print(f"{self.__class__.__name__}: Cannot create directory {log_dir} because {e}. ", end="",
                      file=sys.stderr)  # noqa: E501
                # fmt: on
                print(f"Defaulting to {log_dir}.", file=sys.stderr)

        log_file = os.path.join(log_dir, log_name) + ".log"

        # Create file handler for logging to a file (log all five levels)
        self.file_handler = logging.FileHandler(log_file)
        self.file_handler.setLevel(logging.DEBUG)
        self.file_handler.setFormatter(formatter)
        self.addHandler(self.file_handler)

    def has_console_handler(self):
        return len([h for h in self.handlers if type(h) == logging.StreamHandler]) > 0

    def enable_console_output(self):
        if self.has_console_handler():
            return
        self.addHandler(self.stdout_handler)

    def get_debug_log(self, df_data, df_name="???"):
        assert self.debug_mode in [
            None,
            "count",
            "show",
            "count_and_show",
        ], f"debug_mode = {self.debug_mode}, but it must be one of: None, count, show, count_and_show"

        if self.debug_mode is None:
            msg = f"Checkpoint {df_name} looks good"
        elif self.debug_mode == "count":
            df_count = df_data.count()
            msg = f"Dataframe {df_name} count: {df_count}"
        elif self.debug_mode == "show":
            df_show = df_data._jdf.showString(NUMBER_ROW_TO_SHOW, NUMBER_CHAR_TO_SHOW, False)
            msg = f"Dataframe {df_name} show:\n{df_show}"
        else:
            df_count = df_data.count()
            df_show = df_data._jdf.showString(NUMBER_ROW_TO_SHOW, NUMBER_CHAR_TO_SHOW, False)
            msg = f"""Dataframe {df_name} count: {df_count}\n
                      Dataframe {df_name} show:\n{df_show}
                   """

        return msg
