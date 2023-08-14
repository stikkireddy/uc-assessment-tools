import enum
import io
import json
import os

try:
    import re2 as re
except ImportError:
    import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterator, TextIO, List, Dict, Optional, Tuple

import pandas as pd
from databricks.sdk import WorkspaceClient

from assessment.code_scanner.mounts import mounts_iter, temp_valid_prefix


class SourceType(enum.Enum):
    CLUSTER_JSON = "CLUSTER_JSON"
    JOB_SUBMIT_RUN_JSON = "JOB_SUBMIT_RUN_JSON"
    JOB_JSON = "JOB_JSON"
    JOB_RUN_NOW_JSON = "JOB_RUN_NOW_JSON"
    FILE = "FILE"  # we can only really programatically do something here such as find and replace everything else is
    # an alert


def enum_to_string_factory(data):
    def convert_value(obj):
        if isinstance(obj, enum.Enum):
            return obj.value
        return obj

    return dict((k, convert_value(v)) for k, v in data)


@dataclass
class IssueSource:
    source_type: SourceType
    source_metadata: Optional[Dict[str, str]] = None


@dataclass
class Issue:
    issue_type: str
    issue_detail: str
    issue_source: IssueSource
    line_number: Optional[int] = None
    matched_regex: Optional[str] = None
    matched_line: Optional[str] = None
    matched_value: Optional[str] = None

    @staticmethod
    def issues_to_df(issues: Iterator['Issue']) -> pd.DataFrame:
        return pd.DataFrame([asdict(issue, dict_factory=enum_to_string_factory) for issue in issues])


@dataclass
class IssueInfo:
    issue_type: str
    issue_detail: str


# Sorted by selectivity
issue_cfg: Dict[str, IssueInfo] = {
    # r"""["']dbfs:\/mnt\/[\w]+\/["']""": IssueInfo("MOUNT_USE", "SIMPLE"),
    # r"""["']\/dbfs\/mnt\/[\w]+\/["']""": IssueInfo("MOUNT_USE", "SIMPLE"),
    r"""dbfs:\/mnt\/[\w]+\/""": IssueInfo("NON_MATCHING_MOUNT_USE", "MAYBE"),  # dbfs:/mnt/path/
    r"""\/dbfs\/mnt\/[\w]+\/""": IssueInfo("NON_MATCHING_MOUNT_USE", "MAYBE"),  # /dbfs/mnt/path/
    r"""dbfs:\/mnt\/[\w]+""": IssueInfo("NON_MATCHING_MOUNT_USE", "MAYBE"),
    r"""\/mnt\/[\w]+""": IssueInfo("NON_MATCHING_MOUNT_USE", "MAYBE"),
    # dbfs:/mnt/path  -- we dont know if they are manipulating mount base path via variables
    r"""\/dbfs\/mnt\/[\w]+""": IssueInfo("NON_MATCHING_MOUNT_USE", "MAYBE"),
    # /dbfs/mnt/path -- we dont know if they are manipulating mount base path via variables
    r"""\/dbfs\/mnt\/""": IssueInfo("NON_MATCHING_MOUNT_USE", "CANNOT_CONVERT"),
    # /dbfs/mnt/ -- we dont know what the mount is
    r"""["']\/dbfs/""": IssueInfo("DBFS_USE", "NOT_POSSIBLE"),  # /dbfs/ -- we just know they use dbfs
    r"""udf\(""": IssueInfo("UDF_USE", "FOUND_UDF"),  # @udf(
    r"""# MAGIC %scala""": IssueInfo("SCALA_USE", "FOUND_SCALA"),  # @udf(
    # check if its ufd( variables cannot have ( in them
    r"""spark.udf.register""": IssueInfo("UDF_USE", "FOUND_SQL_BASED_UDF"),  # @udf(
    # check for spark.udf.register
    r"""from pyspark.sql.*udf""": IssueInfo("UDF_USE", "FOUND_PYSPARK_UDF"),
    r"""import org.apache.spark.sql.functions.*udf""": IssueInfo("SCALA_UDF_USE", "FOUND_SCALA_UDF"),
    # check for spark.udf.register
    #  TODO
    # r"""ONLY dbfs:/mnt""": IssueInfo("MOUNT_USE", "NOT_POSSIBLE"),
    # r"""/dbfs/<something other than mnt>""": IssueInfo("MOUNT_USE", "NOT_A_MOUNT"),
    # r"""dbfs:/<something else than mnt>""": IssueInfo("MOUNT_USE", "NOT_A_MOUNT"),
}


def generate_file_name_issue(issue_source: IssueSource, file_name: str) -> Optional[Issue]:
    if file_name.endswith(".scala"):
        return Issue(line_number=None, matched_regex=None, matched_value=None,
                     issue_source=issue_source,
                     issue_type="FOUND_SCALA_FILE", issue_detail="found scala file potentially need enhanced clusters!")
    return None


def also_contains_other_cloud_stuff(line: str) -> bool:
    if any([cloud in line for cloud in ["wasbs://", "abfss://", "s3a://", "adl://", "s3n://", "s3://", "gs://"]]):
        return True
    return False


def is_this_a_fuse_mount(match_value: str, line: str) -> bool:
    fuse = f"/dbfs{match_value}"
    if fuse in line:
        return True
    return False

def get_exact_match(idx, line, issue_source: IssueSource) -> Optional[Issue]:
    for mnt in mounts_iter(temp_valid_prefix):
        r, simple_match = mnt.find_simple_match(line)
        if simple_match is not None:
            if is_this_a_fuse_mount(simple_match, line):
                issue_detail = "MAYBE_FUSE_MOUNT_NEEDS_VOLUMES"
            elif also_contains_other_cloud_stuff(line):
                issue_detail = "MAYBE_FOUND_OTHER_CLOUD_PROTOCOLS"
            else:
                issue_detail = "SIMPLE"
            return Issue(line_number=int(idx) + 1, matched_regex=r,
                        matched_value=simple_match,
                        matched_line=line.strip(),
                        issue_source=issue_source,
                        issue_type="MATCHING_MOUNT_USE",
                        issue_detail=issue_detail)
        r, maybe_match = mnt.find_maybe_match(line)
        if maybe_match is not None:
            return Issue(line_number=int(idx) + 1, matched_regex=r,
                        matched_value=maybe_match,
                        matched_line=line.strip(),
                        issue_source=issue_source,
                        issue_type="MATCHING_MOUNT_USE",
                        issue_detail="MAYBE")
        r, cannot_convert_match = mnt.find_cannot_convert_match(line)
        if cannot_convert_match is not None:
            return Issue(line_number=int(idx) + 1, matched_regex=r,
                        matched_value=cannot_convert_match,
                        matched_line=line.strip(),
                        issue_source=issue_source,
                        issue_type="MATCHING_MOUNT_USE",
                        issue_detail="CANNOT_CONVERT")
    return None

def generate_issues(content: TextIO, issue_regexprs: Dict[str, IssueInfo],
                    issue_source: IssueSource,
                    file_name: Optional[str] = None):
    # handle scala file issue
    # handle udf import issue or scala udf import issue
    # content can be file, dbfs, or file from git, json
    if file_name is not None:
        potential_file_name_issue = generate_file_name_issue(issue_source,
                                                             issue_source.source_metadata.get("file_path"))
        if potential_file_name_issue is not None:
            yield potential_file_name_issue
    lines = content.read().splitlines()
    for idx, line in enumerate(lines):
        # first go through actual mounts in workspace
        # then scan the regexprs which are all flags or indicators but are not actually able to be replaced
        # identify if exact / simple match was found, if so break
        # if not then scan through all the regexes
        # TODO: valid prefix is different between clouds
        #
        # if fount_mount_exact_match:
        #     continue
        for r, info in issue_regexprs.items():
            regex_res = re.search(r, line)
            if regex_res is not None:
                issue = Issue(line_number=int(idx) + 1, matched_regex=r,
                            matched_value=regex_res.group(0),
                            matched_line=line.strip(),
                            issue_source=issue_source,
                            issue_type=info.issue_type,
                            issue_detail=info.issue_detail)
                # if its a MOUNT_USE, try to see if its an exact match for a specific mount in ws
                if issue.issue_type == "NON_MATCHING_MOUNT_USE":
                    exact_match = get_exact_match(idx, line, issue_source)
                    if exact_match is not None:
                        issue = exact_match
                yield issue
                break


class CodeStrategy(ABC):
    # Goal for all code strategy is to dump the files somewhere in the local file system to scan
    # we will have different implementations for dbfs, adls, git repositories, s3, etc
    # implement download_code_directories if you are downloading files from somewhere to local file system
    # implement download_content_items if you are downloading files from api into memory

    @abstractmethod
    def iter_content(self) -> Iterator[Tuple[IssueSource, TextIO]]:
        pass

    def iter_issues(self) -> Iterator[Issue]:
        for issue_source, content_ in self.iter_content():
            yield from generate_issues(content_, issue_cfg, issue_source=issue_source, file_name=None)

    def to_df(self) -> pd.DataFrame:
        return Issue.issues_to_df(self.iter_issues())


class LocalFSCodeStrategy(CodeStrategy):

    def __init__(self, directories: List[Path]):
        self.directories = directories

    @staticmethod
    def get_path(src: IssueSource) -> str:
        return src.source_metadata.get("file_path")

    def iter_content(self):
        for code_dir in self.directories:
            for root, dirs, files in os.walk(str(code_dir)):
                if '.git' in dirs:
                    dirs.remove('.git')
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        fp = Path(file_path).open("r", encoding="utf-8")
                        yield IssueSource(SourceType.FILE, source_metadata={
                            "file_path": file_path
                        }), fp
                    except OSError:
                        pass


class TestingCodeStrategyClusters(CodeStrategy):

    def __init__(self, api_client: WorkspaceClient):
        self.api_client = api_client

    def iter_content(self):
        for cluster in self.api_client.clusters.list():
            yield (IssueSource(SourceType.CLUSTER_JSON, source_metadata={"cluster_id": cluster.cluster_id}),
                   io.StringIO(json.dumps(cluster.as_dict(), indent=2)))
