import functools
import io

from databricks.sdk import WorkspaceClient

from assessment.code_scanner.multi_ws import WorkspaceContextManager

try:
    import re2 as re
except ImportError:
    import re

from dataclasses import dataclass
from typing import Optional, List, Iterator, Tuple, Callable

import pandas as pd

from assessment.code_scanner.utils import get_dbutils, log, FakeMount, get_spark

# TODO: remove this hard coded value
temp_valid_prefix = "abfss"


@dataclass
class Mount:
    target: str
    raw_src: str
    is_mount_valid: bool
    simple: Optional[List[str]] = None
    maybe: Optional[List[str]] = None
    cannot_convert: Optional[List[str]] = None
    org_id: Optional[str] = None
    workspace_url: Optional[str] = None

    @staticmethod
    def _r_search(r, inp) -> Optional[str]:
        regex_res = re.search(r, inp)
        if regex_res is not None:
            return regex_res.group(0)

    # TODO: Return directly an issue if found rather than a match or atleast issue type
    def find_simple_match(self, inp) -> Optional[Tuple[Optional[str], Optional[str]]]:
        for r in (self.simple or []):
            if r is None or r == "":
                continue
            res = self._r_search(r, inp)
            if res is not None:
                return r, res
        return None, None

    def find_maybe_match(self, inp) -> Optional[Tuple[Optional[str], Optional[str]]]:
        for r in (self.maybe or []):
            if r is None or r == "":
                continue
            res = self._r_search(r, inp)
            if res is not None:
                return r, res
        return None, None

    # TODO: Maybe we can indicate the protocol to indicate why it is cannot convert
    def find_cannot_convert_match(self, inp) -> Optional[Tuple[Optional[str], Optional[str]]]:
        for r in (self.cannot_convert or []):
            if r is None or r == "":
                continue
            res = self._r_search(r, inp)
            if res is not None:
                return r, res
        return None, None

    @staticmethod
    def from_csv_bytes(csv_bytes: bytes) -> List["Mount"]:
        mounts = []
        try:
            for row in pd.read_csv(io.BytesIO(csv_bytes)).to_dict(orient="records"):
                mounts.append(Mount(**row))
        except Exception as e:
            log.error("Error parsing mounts csv: %s", e)
        return mounts

    @staticmethod
    def from_pdf(pdf: pd.DataFrame) -> List["Mount"]:
        mounts = []
        for row in pdf.to_dict(orient="records"):
            mounts.append(Mount(**row))
        return mounts


def variations(mnt_path):
    mnt_path = mnt_path.rstrip("/")
    return [
        f"dbfs:{mnt_path}/",
        f"{mnt_path}/",
    ], [
        f"/dbfs{mnt_path}/",  # looks simple if target is a volume but cannot convert for external locations
        f"dbfs:{mnt_path}",
        f"/dbfs{mnt_path}",
        mnt_path
    ]


def custom_mount_sort(mnt):
    return -len(mnt.mountPoint), mnt.mountPoint


@functools.lru_cache
def get_mounts() -> List:
    return list(sorted(get_dbutils().fs.mounts(), key=custom_mount_sort))


@functools.lru_cache
def log_invalid_mounts(src):
    log.info("Found mount with invalid source: %s", src)


@functools.lru_cache
def log_reserved_mounts(src):
    log.info("Found mount with reserved source: %s", src)


def make_mount(mnt: FakeMount, valid_prefix: str) -> Optional[Mount]:
    if mnt.source in [
        "DatabricksRoot", "DbfsReserved", "UnityCatalogVolumes", "databricks/mlflow-tracking",
        "databricks-datasets", "databricks/mlflow-registry", "databricks-results"
    ]:
        log_reserved_mounts(mnt.source)
        return
    if mnt.source.startswith(valid_prefix):
        simple, maybe = variations(mnt.mountPoint)
        return Mount(target=mnt.source, raw_src=mnt.mountPoint,
                     is_mount_valid=True, simple=simple, maybe=maybe)
    else:
        cannot_convert_1, cannot_convert_2 = variations(mnt.mountPoint)
        log_invalid_mounts(mnt.source)
        return Mount(target=mnt.source, raw_src=mnt.mountPoint,
                     is_mount_valid=False, cannot_convert=cannot_convert_2 + cannot_convert_1)


def mounts_iter(valid_prefix: str) -> Iterator[Mount]:
    for mnt in get_mounts():
        mount = make_mount(mnt, valid_prefix)
        spark = get_spark()
        ws_url = spark.conf.get("spark.databricks.workspaceUrl")
        org_id = spark.conf.get("spark.databricks.clusterUsageTags.orgId")
        if mount is not None:
            mount.org_id = org_id
            mount.workspace_url = ws_url
            yield mount


def remote_get_mounts(client: WorkspaceClient, cluster_id: str, set_state: Optional[Callable] = None) \
        -> Tuple[List[FakeMount], str, str]:
    with WorkspaceContextManager(client, cluster_id, set_state) as ws:
        mounts_df = ws.execute_python("display(dbutils.fs.mounts())", as_pdf=True)
        assert mounts_df.shape[0] == int(ws.execute_python("len(dbutils.fs.mounts())").data), "Mounts count mismatch!"
        workspace_url = ws.get_spark_config("spark.databricks.workspaceUrl")
        org_id = ws.get_spark_config("spark.databricks.clusterUsageTags.orgId")
        return [FakeMount(**row) for row in mounts_df.to_dict(orient="records")], workspace_url, org_id


def remote_mounts_iter(client: WorkspaceClient, cluster_id: str, valid_prefix: str,
                       set_state: Optional[Callable] = None) -> Iterator[Mount]:
    mnts, workspace_url, org_id = remote_get_mounts(client, cluster_id, set_state)
    for mnt in mnts:
        mount = make_mount(mnt, valid_prefix)
        if mount is not None:
            mount.org_id = org_id
            mount.workspace_url = workspace_url
            yield mount

def remote_mounts_pdf(client: WorkspaceClient, cluster_id: str, valid_prefix: str,
                      set_state: Optional[Callable] = None) -> pd.DataFrame:
    return pd.DataFrame(remote_mounts_iter(client, cluster_id, valid_prefix, set_state))

def mounts_pdf(valid_prefix: str) -> pd.DataFrame:
    return pd.DataFrame(mounts_iter(valid_prefix))


def mounts_from_pdf(mounts_df: pd.DataFrame, valid_prefix: str) -> Iterator[Mount]:
    for row in mounts_df.to_dict(orient="records"):
        ws_url = row.get("workspace_url")
        org_id = row.get("org_id")
        mnt_path = row.get("raw_src")
        mnt_target = row.get("target")
        mount = make_mount(FakeMount(source=mnt_target, mountPoint=mnt_path), valid_prefix)
        mount.org_id = org_id
        mount.workspace_url = ws_url
        yield mount
