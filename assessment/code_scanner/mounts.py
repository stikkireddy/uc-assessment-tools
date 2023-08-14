import functools
try:
    import re2 as re
except ImportError:
    import re

from dataclasses import dataclass
from typing import Optional, List, Iterator, Tuple

import pandas as pd

from assessment.code_scanner.utils import get_dbutils

# TODO: remove this hard coded value
valid_prefix = "abfss"


@dataclass
class Mount:
    target: str
    raw_src: str
    is_mount_valid: bool
    simple: Optional[List[str]] = None
    maybe: Optional[List[str]] = None
    cannot_convert: Optional[List[str]] = None

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


@functools.lru_cache
def get_mounts() -> List:
    return get_dbutils().fs.mounts()

def mounts_iter(valid_prefix: str) -> Iterator[Mount]:
    for mnt in get_mounts():
        if mnt.source in [
            "DatabricksRoot", "DbfsReserved", "UnityCatalogVolumes", "databricks/mlflow-tracking",
            "databricks-datasets", "databricks/mlflow-registry", "databricks-results"
        ]:
            continue
        if mnt.source.startswith(valid_prefix):
            simple, maybe = variations(mnt.mountPoint)
            yield Mount(target=mnt.source, raw_src=mnt.mountPoint,
                        is_mount_valid=True, simple=simple, maybe=maybe)
        else:
            cannot_convert_1, cannot_convert_2 = variations(mnt.mountPoint)
            yield Mount(target=mnt.source, raw_src=mnt.mountPoint,
                        is_mount_valid=False, cannot_convert=cannot_convert_2 + cannot_convert_1)


def mounts_pdf(valid_prefix: str) -> pd.DataFrame:
    return pd.DataFrame(mounts_iter(valid_prefix))

# if __name__ == "__main__":
# dbutils = FakeDBUtils()
# dbutils.fs.fake_mounts = [
#     FakeMounts(source="abfss://container@stoarge.windows.com", mountPoint="/mnt/some_location"),
# ]
# # for mount in dbutils.fs.mounts():
# #     print(mount)
# for mnt in mounts_iter(dbutils, valid_prefix):
#     print(mnt.find_simple_match("dbfs:/mnt/some_location/someotherpath"))

# for mnt in mounts_iter(fake, valid_prefix):
