import json
from dataclasses import dataclass
from typing import List, Optional

from databricks.sdk import WorkspaceClient


def get_ws_browser_hostname() -> Optional[str]:
    if in_dbx_notebook():
        _dbutils = get_dbutils()
        return json.loads(_dbutils.notebook.entry_point.getDbutils().notebook().getContext().toJson()).get("tags", {})\
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
