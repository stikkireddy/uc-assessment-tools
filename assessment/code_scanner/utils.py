import json
import logging
import os
import time
from dataclasses import dataclass
from typing import List, Optional

from databricks.sdk import WorkspaceClient


def get_ws_browser_hostname() -> Optional[str]:
    if in_dbx_notebook():
        _dbutils = get_dbutils()
        return json.loads(_dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()).get("tags", {}) \
            .get("browserHostName")

    return None


def get_ws_client(default_profile="uc-assessment-azure"):
    if in_dbx_notebook():
        _dbutils = get_dbutils()
        host = _dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None)
        token = _dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
        return WorkspaceClient(host=host, token=token)
    return WorkspaceClient(profile=default_profile)


def get_dbutils():
    try:
        import IPython
        ip_shell = IPython.get_ipython()
        if ip_shell is None:
            return False
        _dbutils = ip_shell.ns_table["user_global"]["dbutils"]
        if _dbutils is not None:
            return _dbutils
    except (ImportError, KeyError, AttributeError):
        pass
    # we are not in databricks and need testing
    dbutils = FakeDBUtils()
    dbutils.fs.fake_mounts = [
        FakeMount(source="abfss://container@stoarge.windows.com/some_src", mountPoint="/mnt/source"),
        FakeMount(source="abfss://container@stoarge.windows.com/some_dest", mountPoint="/mnt/destination"),
        FakeMount(source="abfss://container@stoarge.windows.com/some_location", mountPoint="/mnt/some_location"),
        FakeMount(source="wasbs://container@stoarge.windows.com/some_path", mountPoint="/mnt/some_other_location"),
        FakeMount(source="abfss://discovery@stoarge.windows.com/ml_discovery", mountPoint="/mnt/ADLS_Discovery/"),
        FakeMount(source="abfss://ADLS_MLOps@stoarge.windows.com/some_mlops", mountPoint="/mnt/ADLS_MLOps"),
        *[FakeMount(source=f"abfss://fake_container_{i}@stoarge.windows.com/fake_mnt_{i}",
                    mountPoint=f"/mnt/fake_mnt_{i}") for i in range(150)]
        # blob not possible to migrate
    ]
    return dbutils


def in_dbx_notebook():
    try:
        import IPython

        ip_shell = IPython.get_ipython()
        if ip_shell is None:
            return False
        _dbutils = ip_shell.ns_table["user_global"]["dbutils"]
        if _dbutils is not None:
            return True
    except (ImportError, KeyError, AttributeError):
        return False

    return False


class FakeDBUtils:

    def __init__(self):
        self.fs = FakeFS()


@dataclass
class FakeMount:
    source: str
    mountPoint: str


class FakeFS:

    def __init__(self):
        self.fake_mounts = []

    def mounts(self) -> List[FakeMount]:
        return self.fake_mounts


def setup_logger(log_file):
    # Create the logs folder if it doesn't exist
    logs_folder = "logs"
    if not os.path.exists(logs_folder):
        os.makedirs(logs_folder)

    # Create a logger
    logger = logging.getLogger('assessment')
    logger.setLevel(logging.DEBUG)

    # Create a file handler and set up the formatter
    file_handler = create_file_handler(log_file)
    set_up_formatter(file_handler)

    # Add the handler to the logger
    logger.addHandler(file_handler)

    return logger


def create_file_handler(log_file):
    logs_folder = "logs"
    log_file_path = os.path.join(logs_folder, log_file)

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.DEBUG)

    return file_handler


# class CustomTimeFormatter(logging.Formatter):
#     converter = datetime.fromtimestamp
#
#     def formatTime(self, record, datefmt=None):
#         ct = self.converter(record.created)
#         if datefmt:
#             s = ct.strftime(datefmt)
#         else:
#             t = ct.strftime("%Y-%m-%d %H:%M:%S %Z")
#             parts = t.split(" ")
#             s = "%s %s.%03d %s" % (parts[0], parts[1], record.msecs, parts[2])
#         return s


def set_up_formatter(handler):
    logging.Formatter.converter = time.gmtime
    formatter = logging.Formatter(
        "[%(asctime)s.%(msecs)03d UTC] [%(levelname)s] {%(module)s.py:%(funcName)s:%(lineno)d} - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )
    handler.setFormatter(formatter)


def change_log_filename(logger, new_log_file):
    # Remove existing handlers from the logger
    for handler in logger.handlers:
        logger.removeHandler(handler)

    # Create a new file handler with the updated log filename
    file_handler = create_file_handler(new_log_file)

    # Set up the formatter for the new handler
    set_up_formatter(file_handler)

    # Add the new handler to the logger
    logger.addHandler(file_handler)


log = setup_logger("default_logs.txt")
