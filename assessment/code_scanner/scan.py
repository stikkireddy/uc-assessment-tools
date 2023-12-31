import ast
import enum
import io
import json
import os

from assessment.code_scanner.utils import log

try:
    import re2 as re
except ImportError:
    import re
from abc import ABC, abstractmethod
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Iterator, TextIO, List, Dict, Optional, Tuple, Union, Callable

import pandas as pd
from databricks.sdk import WorkspaceClient

from assessment.code_scanner.mounts import mounts_iter, temp_valid_prefix, Mount


class SourceType(enum.Enum):
    CLUSTER_JSON = "CLUSTER_JSON"
    JOB_SUBMIT_RUN_JSON = "JOB_SUBMIT_RUN_JSON"
    JOB_JSON = "JOB_JSON"
    JOB_RUN_NOW_JSON = "JOB_RUN_NOW_JSON"
    FILE = "FILE"  # we can only really programatically do something here such as find and replace everything else is
    # an alert


def enum_to_string_factory(data):
    def convert_value(obj):
        # If a single field is too long, truncate it
        if isinstance(obj, str):
            if len(obj) > 10000:
                return f"{obj[:10000]}..."
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
    workspace_url: Optional[str] = None

    @staticmethod
    def issues_to_df(issues: Union[Iterator['Issue'], List['Issue']]) -> pd.DataFrame:
        issues = [asdict(issue, dict_factory=enum_to_string_factory) for issue in issues]

        if len(issues) > 0:
            return pd.DataFrame(issues)
        return pd.DataFrame(columns=["issue_type", "issue_detail", "issue_source", "line_number", "matched_regex", ])

    @staticmethod
    def from_df(pdf: pd.DataFrame) -> List["Issue"]:
        issues = []
        try:
            for row in pdf.to_dict(orient="records"):
                iss = Issue(**row)
                # Csv will have it as a python string
                issue_source_raw = row.get("issue_source", "{}")
                if isinstance(issue_source_raw, str):
                    issue_src = ast.literal_eval(issue_source_raw)
                    src_type = SourceType[issue_src.get("source_type")]
                    issue_src = IssueSource(**issue_src)
                    issue_src.source_type = src_type
                else:
                    issue_src = IssueSource(**issue_source_raw)
                iss.issue_source = issue_src
                issues.append(iss)
        except Exception as e:
            log.error("Error parsing issues csv: %s", e)
        finally:
            return issues

    @staticmethod
    def from_csv_bytes(csv_bytes: bytes) -> Iterator['Issue']:
        pdf = pd.read_csv(io.BytesIO(csv_bytes))
        return Issue.from_df(pdf)

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
    r"""# MAGIC %scala""": IssueInfo("SCALA_USE", "FOUND_SCALA"),
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


def has_multiple_occurrences(string, inputs: List[str]) -> bool:
    occurrence_count = {}

    for input_str in inputs:
        count = string.count(input_str)
        occurrence_count[input_str] = count

        if count > 1:
            return True

    return False

def get_exact_match(idx, line,
                    issue_source: IssueSource,
                    discovered_mounts: Optional[List[Mount]] = None) -> Optional[List[Issue]]:
    multiple_issues = []
    has_mult_occurs = has_multiple_occurrences(line, ["/mnt"])
    for mnt in discovered_mounts or mounts_iter(temp_valid_prefix):
        r, simple_match = mnt.find_simple_match(line)
        if simple_match is not None:
            if is_this_a_fuse_mount(simple_match, line):
                issue_detail = "MAYBE_FUSE_MOUNT_NEEDS_VOLUMES"
            elif also_contains_other_cloud_stuff(line):
                issue_detail = "MAYBE_FOUND_OTHER_CLOUD_PROTOCOLS"
            else:
                issue_detail = "SIMPLE"
            multiple_issues.append(Issue(line_number=int(idx) + 1, matched_regex=r,
                                         matched_value=simple_match,
                                         matched_line=line.strip(),
                                         issue_source=issue_source,
                                         issue_type="MATCHING_MOUNT_USE",
                                         issue_detail=issue_detail))
            if has_mult_occurs is False:
                return multiple_issues
            continue
        r, maybe_match = mnt.find_maybe_match(line)
        if maybe_match is not None:
            if f"get_uc_mount_target('{maybe_match}'," in line:
                multiple_issues.append(Issue(line_number=int(idx) + 1, matched_regex=r,
                                             matched_value=maybe_match,
                                             matched_line=line.strip(),
                                             issue_source=issue_source,
                                             issue_type="MATCHING_MOUNT_USE",
                                             issue_detail="ALREADY_CONVERTED"))
            else:
                multiple_issues.append(Issue(line_number=int(idx) + 1, matched_regex=r,
                                             matched_value=maybe_match,
                                             matched_line=line.strip(),
                                             issue_source=issue_source,
                                             issue_type="MATCHING_MOUNT_USE",
                                             issue_detail="MAYBE"))
            if has_mult_occurs is False:
                return multiple_issues
            continue
        r, cannot_convert_match = mnt.find_cannot_convert_match(line)
        if cannot_convert_match is not None:
            multiple_issues.append(Issue(line_number=int(idx) + 1, matched_regex=r,
                                         matched_value=cannot_convert_match,
                                         matched_line=line.strip(),
                                         issue_source=issue_source,
                                         issue_type="MATCHING_MOUNT_USE",
                                         issue_detail="CANNOT_CONVERT"))
            if has_mult_occurs is False:
                return multiple_issues
            continue
    return multiple_issues


def handle_magic(issue: Issue) -> Issue:
    if issue.issue_type in ["MATCHING_MOUNT_USE", "NON_MATCHING_MOUNT_USE"]:
        if issue.matched_line.startswith("# MAGIC"):
            issue.issue_detail = "CANNOT_CONVERT_MAGIC_CMD"
    return issue


def handle_unsupported_file_types(issue: Issue):
    if not issue.issue_source.source_metadata.get("file_path").endswith(".py"):
        file_extension = Path(issue.issue_source.source_metadata.get("file_path")).suffix.upper().replace(".", "")
        issue.issue_detail = "CANNOT_CONVERT_{}_FILE".format(file_extension)
    return issue


def generate_issues(content: TextIO, issue_regexprs: Dict[str, IssueInfo],
                    issue_source: IssueSource,
                    discovered_mounts: Optional[List[Mount]] = None,
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
                    exact_match = get_exact_match(idx, line, issue_source, discovered_mounts)
                    if exact_match is not None and exact_match != []:
                        for iss in exact_match:
                            yield handle_unsupported_file_types(handle_magic(iss))
                    else:
                        # TODO: We may not have any exact matches so just yield the generic capture of /mnt or /dbfs
                        yield handle_unsupported_file_types(handle_magic(issue))
                else:
                    # TODO: We assume right now that all magic commands are SQL, but we could see instances of python or
                    #  scala magic commands that could be handled.
                    yield handle_unsupported_file_types(handle_magic(issue))
                break


class CodeStrategy(ABC):
    # Goal for all code strategy is to dump the files somewhere in the local file system to scan
    # we will have different implementations for dbfs, adls, git repositories, s3, etc
    # implement download_code_directories if you are downloading files from somewhere to local file system
    # implement download_content_items if you are downloading files from api into memory
    def __init__(self, discovered_mounts: Optional[List[Mount]] = None):
        self._discovered_mounts = discovered_mounts

    @abstractmethod
    def iter_content(self) -> Iterator[Tuple[IssueSource, TextIO]]:
        pass

    def iter_issues(self) -> Iterator[Issue]:
        for issue_source, content_ in self.iter_content():
            try:
                log.info(f"Scanning {issue_source.source_metadata.get('relative_file_path')}")
                yield from generate_issues(content_, issue_cfg, issue_source=issue_source, file_name=None,
                                           discovered_mounts=self._discovered_mounts)
            except (OSError, UnicodeDecodeError):
                log.error(
                    f"Unable to open file {issue_source.source_metadata.get('relative_file_path')}; "
                    f"src: {str(issue_source)}")
                pass

    def to_df(self) -> pd.DataFrame:
        return Issue.issues_to_df(self.iter_issues())


class LocalFSCodeStrategy(CodeStrategy):

    def __init__(self, directories: List[Path],
                 set_max_prog: Callable[[int], None] = None,
                 set_curr_prog: Callable[[int], None] = None,
                 set_curr_file: Callable[[str], None] = None,
                 discovered_mounts: Optional[List[Mount]] = None):
        super().__init__(discovered_mounts=discovered_mounts)
        self.set_curr_file = set_curr_file
        self.set_curr_prog = set_curr_prog
        self.set_max_prog = set_max_prog
        self.directories = directories

    @staticmethod
    def get_path(src: IssueSource) -> str:
        return src.source_metadata.get("file_path")

    @staticmethod
    def get_relative_path(src: IssueSource) -> str:
        return src.source_metadata.get("relative_file_path")

    @staticmethod
    def file_count(directories: List[Path]) -> int:
        file_count = 0

        for code_dir in directories:
            for root, dirs, files in os.walk(str(code_dir)):
                if '.git' in dirs:
                    dirs.remove('.git')
                file_count += len(files)
        return file_count

    def iter_content(self):
        if self.set_max_prog is not None:
            self.set_max_prog(self.file_count(self.directories))
        curr_prog = 0
        for code_dir in self.directories:
            code_dir_with_suffix = str(code_dir).rstrip("/") + "/"
            for root, dirs, files in os.walk(str(code_dir)):

                if '.git' in dirs:
                    dirs.remove('.git')
                for file in files:
                    file_path = os.path.join(root, file)
                    try:
                        if self.set_curr_file is not None:
                            self.set_curr_file(file_path.replace(code_dir_with_suffix, ""))
                        fp = Path(file_path).open("r", encoding="utf-8")
                        yield IssueSource(SourceType.FILE, source_metadata={
                            "file_path": file_path,
                            "relative_file_path": file_path.replace(code_dir_with_suffix, "")
                        }), fp
                    except (OSError, UnicodeDecodeError):
                        log.error(f"Unable to open file {file_path}")
                    finally:
                        if self.set_curr_prog is not None:
                            curr_prog += 1
                            self.set_curr_prog(curr_prog)

        # end
        if self.set_curr_file is not None:
            self.set_curr_file("")
        if self.set_max_prog is not None:
            self.set_max_prog(0)
        if self.set_curr_prog is not None:
            self.set_curr_prog(0)


class TestingCodeStrategyClusters(CodeStrategy):

    def __init__(self, api_client: WorkspaceClient):
        super().__init__()
        self.api_client = api_client

    def iter_content(self):
        for cluster in self.api_client.clusters.list():
            yield (IssueSource(SourceType.CLUSTER_JSON, source_metadata={"cluster_id": cluster.cluster_id}),
                   io.StringIO(json.dumps(cluster.as_dict(), indent=2)))
