import enum
import io
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, TextIO, List

from assessment.code_scanner.scan import IssueSource, Issue, SourceType, LocalFSCodeStrategy
from assessment.code_scanner.utils import log


@dataclass
class IssueReplaceMapping:
    matching_regex: str
    match_mapping: Dict[str, str]

    @classmethod
    def from_mount(cls, mnt, mode="SIMPLE") -> List['IssueReplaceMapping']:
        if mode == "SIMPLE":
            target = f'{mnt.source.rstrip("/")}/'
            src = f'{mnt.mountPoint.rstrip("/")}/'
            dbfs_prefix_src = f"dbfs:{src}"
            return [
                cls(matching_regex=src, match_mapping={src: target}),
                cls(matching_regex=dbfs_prefix_src, match_mapping={dbfs_prefix_src: target}),
            ]


class InputType(enum.Enum):
    FILE = "FILE"
    WORKSPACE_NOTEBOOK = "WORKSPACE_NOTEBOOK"
    STORAGE_FILE = "DBFS_FILE"


class OutputType(enum.Enum):
    FILE = "FILE"
    WORKSPACE_NOTEBOOK = "WORKSPACE_NOTEBOOK"
    STORAGE_FILE = "DBFS_FILE"


class OutputWriter(ABC):

    @abstractmethod
    def write(self, input_source: IssueSource, content: TextIO):
        pass


class FileOutputWriter(OutputWriter):

    def __init__(self, working_dir: str):
        self.working_dir = working_dir

    def write(self, input_source: IssueSource, content: TextIO):
        if input_source.source_metadata.get("relative_file_path") is None:
            raise ValueError("file_path is not defined in source_metadata")
        with (Path(self.working_dir) / input_source.source_metadata.get("relative_file_path")) \
                .open("w", encoding="utf-8") as f:
            f.write(content.read())


class InputReader(ABC):

    @abstractmethod
    def open(self, input_source: IssueSource) -> TextIO:
        pass


class FileInputReader(InputReader):

    def __init__(self, working_dir: str):
        self.working_dir = working_dir

    def open(self, input_source: IssueSource) -> TextIO:
        if input_source.source_metadata.get("relative_file_path") is None:
            raise ValueError("file_path is not defined in source_metadata")
        return (Path(self.working_dir) / input_source.source_metadata.get("relative_file_path")) \
            .open("r", encoding="utf-8")


class IssueResolverStrategy(ABC):

    def __init__(self, input_reader: InputReader, output_writer: OutputWriter):
        self.input_reader = input_reader
        self.output_writer = output_writer

    @staticmethod
    def search_issue_replace_mapping(issue: Issue, issue_replace_mappings: List[IssueReplaceMapping]):
        for issue_replace_mapping in issue_replace_mappings:
            if issue_replace_mapping.matching_regex == issue.matched_regex:
                return issue_replace_mapping
        return None

    @abstractmethod
    def filter_issues(self, issues: List[Issue]):
        pass

    @abstractmethod
    def handle_issues(self, issues: List[Issue]):
        pass


def is_already_replaced_or_skip(target_replacement, line):
    if line.find(target_replacement) >= 0:
        return True
    if line.find("uc-scan:skip") >= 0:
        return True
    return False

def replace_string_with_custom_function(matched_string: str, line: str,
                                        function_name: str = "get_uc_mount_target") -> str:
    # we assume all lookups for the function call are normalized without trailing slash
    # we also assume that mount lookups also do not start with dbfs: in the dictionaries
    # the function needs to have the type hint: Callable[[str, bool], str]
    # eg: def get_uc_mount_target(path: str, normalize: bool = True) -> str:
    quote_types = ['f"', "f'", 'f"""', "f'''"] + ['"', "'", '"""', "'''"]
    normalized_matched_string = matched_string.rstrip("/")
    for quote_type in quote_types:
        if normalized_matched_string.startswith(quote_type) or line.find(quote_type + normalized_matched_string) != -1:
            # all lookups will be normalized and follow pattern /mnt/<mount_name>/*
            normalized_lookup_string = matched_string.replace("dbfs:", "").rstrip("/")
            function_call_string = f"{function_name}('{normalized_lookup_string}', normalize=True)"
            replacement = function_call_string + " + " + quote_type
            if is_already_replaced_or_skip(function_call_string, line) is True:
                # we have already done a replacement it should skip
                log.info("Skipping replacement: %s with %s in line %s", matched_string, replacement, line)
                return line
            log.info("Attempting to replace: %s with %s in line %s", matched_string, replacement, line)
            replaced_line = line.replace(quote_type + normalized_matched_string, replacement, 1)
            return replaced_line
    return line


class FileIssueSimpleResolver(IssueResolverStrategy):

    def __init__(self,
                 input_reader: InputReader,
                 output_writer: OutputWriter,
                 support_maybes: bool = True):
        super().__init__(input_reader, output_writer)
        self.support_maybes = support_maybes
        self.file_issue_mapping = defaultdict(list)

    def filter_issues(self, issues: List[Issue]):
        # we can only handle file issues which are simple find and replace
        valid_issues = []
        for issue in issues:
            # only replace content if its a file, it is a matching mount use and it is a simple issue or maybe
            # flag is turned on
            if issue.issue_source.source_type == SourceType.FILE and issue.issue_type == "MATCHING_MOUNT_USE":
                if self.support_maybes is True and issue.issue_detail == "MAYBE":
                    valid_issues.append(issue)
                if issue.issue_detail == "SIMPLE":
                    valid_issues.append(issue)
        return valid_issues

    def _replace_content(self, file_path: str) -> io.StringIO:
        src = IssueSource(SourceType.FILE, source_metadata={"relative_file_path": file_path})
        lines = self.input_reader.open(src).readlines()
        issues = self.file_issue_mapping[file_path]
        for issue in issues:
            lines[issue.line_number - 1] = replace_string_with_custom_function(issue.matched_value,
                                                                               lines[issue.line_number - 1])
        return io.StringIO("".join(lines))

    def handle_issues(self, issues: List[Issue]):
        # TODO: need to fix this
        for issue in self.filter_issues(issues):
            self.file_issue_mapping[LocalFSCodeStrategy.get_relative_path(issue.issue_source)].append(issue)
        for file_path, _ in self.file_issue_mapping.items():
            log.info("Replacing issues in file: %s", file_path)
            new_content = self._replace_content(file_path).getvalue()
            self.output_writer.write(IssueSource(SourceType.FILE, source_metadata={"relative_file_path": file_path}),
                                     io.StringIO(new_content))

# if __name__ == "__main__":
#     print(replace_string_with_custom_function("/mnt/meijermount/", '''stgloc = "/mnt/meijermount/dkushari/dkushari_db/ADRMParquet/{0}Stg".format(entity_name)'''))
#     print(replace_string_with_custom_function("/mnt/foo", '''stgloc = get_uc_mount_target('/mnt/meijermount', normalize=True) + "/dkushari/dkushari_db/ADRMParquet/{0}Stg".format(entity_name)'''))
