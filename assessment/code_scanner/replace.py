import enum
import io
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass
from typing import Dict, TextIO, List

from assessment.code_scanner.scan import IssueSource, Issue, SourceType, LocalFSCodeStrategy


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

    def write(self, input_source: IssueSource, content: TextIO):
        if input_source.source_metadata.get("file_path") is None:
            raise ValueError("file_path is not defined in source_metadata")
        with open(input_source.source_metadata.get("file_path"), "w", encoding="utf-8") as f:
            f.write(content.read())


class InputReader(ABC):

    @abstractmethod
    def open(self, input_source: IssueSource) -> TextIO:
        pass


class FileInputReader(InputReader):

    def open(self, input_source: IssueSource) -> TextIO:
        if input_source.source_metadata.get("file_path") is None:
            raise ValueError("file_path is not defined in source_metadata")
        return open(input_source.source_metadata.get("file_path"), "r", encoding="utf-8")


class IssueResolverStrategy(ABC):

    def __init__(self, input_reader: InputReader, output_writer: OutputWriter):
        self.input_reader = input_reader
        self.output_writer = output_writer

    @staticmethod
    def search_issue_replace_mapping(issue: Issue, issue_replace_mappings: List[IssueReplaceMapping]):
        for issue_replace_mapping in issue_replace_mappings:
            if issue_replace_mapping.matching_regex == issue.matched_regex:
                print(issue_replace_mapping, issue)
                return issue_replace_mapping
        return None

    @abstractmethod
    def filter_issues(self, issues: List[Issue]):
        pass

    @abstractmethod
    def handle_issues(self, issues: List[Issue], issue_replace_mappings: List[IssueReplaceMapping]):
        pass


class FileIssueSimpleResolver(IssueResolverStrategy):

    def __init__(self, input_reader: InputReader, output_writer: OutputWriter):
        super().__init__(input_reader, output_writer)
        self.file_issue_mapping = defaultdict(list)

    def filter_issues(self, issues: List[Issue]):
        # we can only handle file issues which are simple find and replace
        return [issue for issue in issues if issue.issue_source.source_type == SourceType.FILE and
                issue.issue_detail == "SIMPLE"]

    def _replace_content(self, file_path: str, issue_replace_mappings: List[IssueReplaceMapping]) -> io.StringIO:
        src = IssueSource(SourceType.FILE, source_metadata={"file_path": file_path})
        lines = self.input_reader.open(src).readlines()
        issues = self.file_issue_mapping[file_path]
        for issue in issues:
            issue_replace_mapping = self.search_issue_replace_mapping(issue, issue_replace_mappings)
            if issue_replace_mapping is not None:
                target_replacement = issue_replace_mapping.match_mapping[issue.matched_value]
                lines[issue.line_number - 1] = lines[issue.line_number - 1].replace(issue.matched_value,
                                                                                    target_replacement)

        return io.StringIO("".join(lines))

    def handle_issues(self, issues: List[Issue], issue_replace_mappings: List[IssueReplaceMapping]):
        # TODO: need to fix this
        for issue in self.filter_issues(issues):
            self.file_issue_mapping[LocalFSCodeStrategy.get_path(issue.issue_source)].append(issue)
        for file_path, issues in self.file_issue_mapping.items():
            new_content = self._replace_content(file_path, issue_replace_mappings).getvalue()
            self.output_writer.write(IssueSource(SourceType.FILE, source_metadata={"file_path": file_path}),
                                     io.StringIO(new_content))
