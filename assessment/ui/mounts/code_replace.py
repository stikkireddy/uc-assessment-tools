from typing import Callable, cast, Optional

import pandas as pd
import solara
from solara.components.file_drop import FileInfo

from assessment.code_scanner.scan import Issue


def get_issues_detail_message(issues: pd.DataFrame) -> str:
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
def CodeFindAndReplace(issues: pd.DataFrame = None, set_issues: Callable[[pd.DataFrame], None] = None):
    uploaded_file_contents, set_uploaded_file_contents = solara.use_state(cast(Optional[bytes], None))
    branch, set_branch = solara.use_state("uc_convert_")
    repo_url, set_repo_url = solara.use_state("")
    user, set_user = solara.use_state("")
    token, set_token = solara.use_state("")

    with solara.Card("Find And Replace"):

        def on_file(file_contents: FileInfo):
            issues_pdf = pd.DataFrame(Issue.from_csv_bytes(file_contents.get("data")))
            print(issues_pdf)
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
                      # on_click=get_issues,
                      disabled=issues is None)

