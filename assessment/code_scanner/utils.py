import dataclasses
import io
import json
import logging
import os
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional, Union

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


def get_spark() -> Union["FakeSpark", "SparkSession"]:
    try:
        import IPython
        ip_shell = IPython.get_ipython()
        if ip_shell is None:
            raise ValueError("Not in IPython")
        _spark = ip_shell.ns_table["user_global"]["spark"]
        if _spark is not None:
            return _spark
    except (ImportError, KeyError, AttributeError, ValueError):
        pass

    # we are not in databricks and need testing
    spark = FakeSpark()
    spark.conf._conf["spark.databricks.workspaceUrl"] = "some-workspace-url"
    spark.conf._conf["spark.databricks.clusterUsageTags.orgId"] = "234141412412"
    return spark


def get_db_base_path() -> str:
    dbutils = get_dbutils()
    if isinstance(dbutils, FakeDBUtils):
        return str(Path(os.getcwd()) / "tmp_db" / "notebook")  # testing locally
    return "/Workspace" + dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()


def get_dbutils() -> Union["FakeDBUtils", "DBUtils"]:
    try:
        import IPython
        ip_shell = IPython.get_ipython()
        if ip_shell is None:
            raise ValueError("Not in IPython")
        _dbutils = ip_shell.ns_table["user_global"]["dbutils"]
        if _dbutils is not None:
            return _dbutils
    except (ImportError, KeyError, AttributeError, ValueError):
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
    dbutils.secrets._secrets = {m.mountPoint: m.source for m in dbutils.fs.fake_mounts
                                if m.source.startswith("abfss://")}
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


@dataclass
class Scope:
    name: str


@dataclass
class SecretMetadata:
    key: str


class FakeSecrets:
    def __int__(self):
        self._secrets = {}
        self._scopes = []

    def get(self, scope: str, key: str) -> Optional[str]:
        if scope in [s.name for s in self.listScopes()]:
            if self._secrets.get(scope) is not None:
                return self._secrets[scope].get(key)
        return None

    def listScopes(self) -> List[Scope]:
        return [Scope(name=scope) for scope in self._scopes]

    def list(self, scope: str) -> List[SecretMetadata]:
        return [SecretMetadata(key=key) for key in self._secrets.get(scope).keys()]


class FakeSpark:

    def __init__(self):
        self.conf = FakeSparkConf()


class FakeSparkConf:

    def __init__(self):
        self._conf = {}

    def get(self, key):
        return self._conf.get(key, None)

    def set(self, key, value):
        self._conf[key] = value


# dbutils.notebook.entry_point.getDbutils().notebook().getContext().notebookPath().get()


class FakeDBUtils:

    def __init__(self):
        self.fs = FakeFS()
        self.secrets = FakeSecrets()


@dataclass(init=False)
class FakeMount:
    source: str
    mountPoint: str

    def __init__(self, **kwargs):
        names = set([f.name for f in dataclasses.fields(self)])
        for k, v in kwargs.items():
            if k in names:
                setattr(self, k, v)


class FakeFS:

    def __init__(self):
        self.fake_mounts = []

    def mounts(self) -> List[FakeMount]:
        return self.fake_mounts


LOGS_FOLDER = "logs/uc-assessment"


def setup_logger(log_file):
    # Create the logs folder if it doesn't exist
    if not os.path.exists(LOGS_FOLDER):
        os.makedirs(LOGS_FOLDER)

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
    log_file_path = os.path.join(LOGS_FOLDER, log_file)

    file_handler = logging.FileHandler(log_file_path)
    file_handler.setLevel(logging.DEBUG)

    return file_handler


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


def zip_bytes(input_bytes, file_name):
    output_buffer = io.BytesIO()

    with zipfile.ZipFile(output_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
        zipf.writestr(file_name, input_bytes)

    compressed_bytes = output_buffer.getvalue()
    return compressed_bytes


log = setup_logger("default_logs.txt")

stdout_handler = logging.StreamHandler()

# Set the formatter for the handler
set_up_formatter(stdout_handler)

# Add the handler to the logger
log.addHandler(stdout_handler)
