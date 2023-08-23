import tempfile
import traceback
from typing import Callable, cast, Optional

import pandas as pd
import solara
from solara.components.file_drop import FileInfo

from assessment.code_scanner.replace import FileIssueSimpleResolver, FileInputReader, FileOutputWriter
from assessment.code_scanner.repos import git_repo
from assessment.code_scanner.scan import Issue
from assessment.code_scanner.utils import get_ws_client, log


def get_issues_detail_message(issues: pd.DataFrame) -> str:
    if issues.shape[0] == 0:
        return "No issues found."
    matching_mount_use_count = issues[issues['issue_type'] == 'MATCHING_MOUNT_USE'].shape[0]
    matching_mount_use_simple_count = issues[
        (issues['issue_type'] == 'MATCHING_MOUNT_USE') & (issues['issue_detail'] == 'SIMPLE')
        ].shape[0]
    matching_mount_use_maybe_count = issues[
        (issues['issue_type'] == 'MATCHING_MOUNT_USE') & (issues['issue_detail'] == 'MAYBE')
        ].shape[0]
    matching_mount_use_cannot_convert_count = issues[
        ~((issues['issue_type'] == 'MATCHING_MOUNT_USE') & (
            (issues['issue_detail'] == 'SIMPLE')
        ))
    ].shape[0]

    return (f"Found {matching_mount_use_count} matching mount use issues. "
            f"{matching_mount_use_simple_count} simple, {matching_mount_use_maybe_count} maybe, "
            f"{matching_mount_use_cannot_convert_count} cannot convert.")


@solara.component
def CodeFindAndReplace(issues: pd.DataFrame = None, set_issues: Callable[[pd.DataFrame], None] = None,
                       repo_url: str = None, set_repo_url: Callable[[str], None] = None,
                       user: str = None, set_user: Callable[[str], None] = None,
                       token: str = None, set_token: Callable[[str], None] = None
                       ):
    uploaded_file_contents, set_uploaded_file_contents = solara.use_state(cast(Optional[bytes], None))
    branch, set_branch = solara.use_state("uc_convert_")

    loading, set_loading = solara.use_state(False)
    error, set_error = solara.use_state("")

    def find_and_replace():
        ws_client = get_ws_client(default_profile="uc-assessment-azure")
        curr_user = ws_client.current_user.me()
        user_name = curr_user.display_name
        email = curr_user.user_name
        set_loading(True)
        set_error("")
        try:
            with tempfile.TemporaryDirectory() as path:
                with git_repo(repo_url, branch, path, email=email, full_name=user_name, delete=False,
                              username=user, password=token):
                    FileIssueSimpleResolver(
                        FileInputReader(working_dir=path),
                        FileOutputWriter(working_dir=path),
                    ).handle_issues(Issue.from_df(issues))
        except Exception as e:
            set_error(str(e))
            log.error(traceback.format_exc())
        finally:
            set_loading(False)

    with solara.Card("Find And Replace"):

        def on_file(file_contents: FileInfo):
            issues_pdf = pd.DataFrame(Issue.from_csv_bytes(file_contents.get("data")))
            set_issues(issues_pdf)
            set_uploaded_file_contents(file_contents.get("data"))

        solara.FileDrop(
            label="Drag and drop a issues csv file here.",
            on_file=on_file,
            lazy=False,  # We will only read the first 100 bytes
        )
        solara.Button("Reload Issues",
                      on_click=lambda: set_issues(pd.DataFrame(Issue.from_csv_bytes(uploaded_file_contents))),
                      style="margin-bottom: 8px")
        if issues is None:
            solara.Error("No issues found!")
        else:
            solara.Info(f"Note: Using {issues.shape[0]} issues. {get_issues_detail_message(issues)}")
            solara.Info("Note: Will only convert matching mount and simple issues so "
                        "download the issues and make modifications in excel.")

        solara.Markdown(f"### Find and Replace in branch: {branch} for repo: {repo_url}")
        solara.InputText("Branch", value=branch, on_value=set_branch,
                         error="Branch name must start with 'uc_convert_'"
                         if not branch.startswith("uc_convert_") else None, continuous_update=True)
        solara.InputText("Repo Url", value=repo_url, on_value=set_repo_url, continuous_update=True)
        solara.InputText("User Name", value=user, on_value=set_user, continuous_update=True)
        solara.InputText("Token", value=token, on_value=set_token, password=True, continuous_update=True)
        solara.Button("Find and Replace", style="margin-bottom: 25px",
                      on_click=find_and_replace,
                      disabled=issues is None or loading is True)

        if loading is True:
            solara.Info("Attempting to find and replace...")
            solara.ProgressLinear(True)
        elif error is not None and error != "":
            solara.Error("Error: " + error)
