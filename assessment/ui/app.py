import os
import tempfile
import traceback
from datetime import datetime
from pathlib import Path
from typing import Callable, cast, List, Optional

import pandas as pd
import solara
import solara.lab
from solara.components.file_drop import FileInfo

from assessment.code_scanner.mounts import mounts_from_pdf, temp_valid_prefix, Mount
from assessment.code_scanner.repos import git_repo
from assessment.code_scanner.scan import LocalFSCodeStrategy, Issue
from assessment.code_scanner.utils import get_ws_client, change_log_filename, log, LOGS_FOLDER
from assessment.ui.assessments.assessments import Assessments
from assessment.ui.components.log_browser import LogBrowser
from assessment.ui.mounts.code_replace import CodeFindAndReplace
from assessment.ui.mounts.mount_scanner_v1 import MountScanner
from assessment.ui.mounts.mount_scanner_v2 import MountScannerV2
from assessment.ui.settings import Settings
from assessment.ui.state import workspace_conf_ini, workspace_url


@solara.component
def RepoScanner(mounts: pd.DataFrame, set_mounts: Callable[[pd.DataFrame], None],
                issues: pd.DataFrame = None, set_issues: Callable[[pd.DataFrame], None] = None,
                repo_url: str = None, set_repo_url: Callable[[str], None] = None,
                user: str = None, set_user: Callable[[str], None] = None,
                token: str = None, set_token: Callable[[str], None] = None
                ):
    # issues, set_issues = solara.use_state(None)
    loading, set_loading = solara.use_state(False)
    max_progress, set_max_progress = solara.use_state(0)
    progress, set_progress = solara.use_state(0)
    error, set_error = solara.use_state("")
    # repo_url, set_repo_url = solara.use_state("")
    # user, set_user = solara.use_state("")
    # token, set_token = solara.use_state("")
    curr_file, set_curr_file = solara.use_state("")
    uploaded_file_contents, set_uploaded_file_contents = solara.use_state(cast(Optional[bytes], None))

    issues: pd.DataFrame
    set_issues: Callable[[pd.DataFrame], None]

    def get_raw_data(csv=False):
        df_copy = issues.copy(deep=True)
        df_copy['workspace_url'] = workspace_url
        if csv is True:
            return df_copy.to_csv(index=False)

        return df_copy.to_parquet(index=False)

    def get_plotly_mounts():
        import plotly.express as px
        # Group and count the occurrences of each issue type and detail combination
        grouped_counts = issues.groupby(['issue_type', 'issue_detail']).size().reset_index(name='count')

        # Combine issue_type and issue_detail columns for coloring
        grouped_counts['color'] = grouped_counts['issue_type'] + ' - ' + grouped_counts['issue_detail']

        # Create a pie chart using Plotly Express
        fig = px.pie(grouped_counts, values='count', names='color', title="Issue Type and Detail Breakdown")

        # Update color scale to match the issue types
        color_scale = px.colors.qualitative.Set1[:len(grouped_counts['issue_type'].unique())]
        fig.update_traces(marker=dict(colors=color_scale))
        return fig

    def get_issues():
        if repo_url is None or repo_url == "":
            set_error("Please enter a repo url")
            return
        set_error("")
        try:
            set_loading(True)
            ws_client = get_ws_client(default_profile="uc-assessment-azure")
            curr_user = ws_client.current_user.me()
            user_name = curr_user.display_name
            email = curr_user.user_name
            with tempfile.TemporaryDirectory() as path:
                with git_repo(repo_url, None, path, email=email, full_name=user_name, delete=True,
                              username=user, password=token):
                    scan = LocalFSCodeStrategy([Path(path)],
                                               discovered_mounts=list(mounts_from_pdf(mounts, temp_valid_prefix)),
                                               set_curr_file=set_curr_file,
                                               set_max_prog=set_max_progress,
                                               set_curr_prog=set_progress)
                    # this identifies all the issues in the repo
                    _issues = []
                    for iss in scan.iter_issues():
                        _issues.append(iss)
                    set_issues(Issue.issues_to_df(_issues))
        except Exception as e:
            log.error(traceback.format_exc())
            set_error(str(e))
        finally:
            set_loading(False)

    with solara.Card("Scan Mounts in Repos"):
        solara.Info("Note: This will ignore mounts: DatabricksRoot, DbfsReserved, UnityCatalogVolumes, "
                    "databricks/mlflow-tracking, databricks-datasets, databricks/mlflow-registry, databricks-results.")

        def on_file(file_contents: FileInfo):
            set_mounts(pd.DataFrame(Mount.from_csv_bytes(file_contents.get("data"))))
            set_uploaded_file_contents(file_contents.get("data"))

        solara.FileDrop(
            label="Drag and drop a mounts csv file here.",
            on_file=on_file,
            lazy=False,  # We will only read the first 100 bytes
        )

        solara.Button("Reload File",
                      on_click=lambda: set_mounts(pd.DataFrame(Mount.from_csv_bytes(uploaded_file_contents))))

        if mounts is not None and mounts.shape[0] > 0:
            solara.Info(f"Note: Using {mounts.shape[0]} mounts.")
        else:
            solara.Error("Note: No mounts found, please load them.")
        solara.InputText("Repo Url", value=repo_url, on_value=set_repo_url)
        solara.InputText("User Name", value=user, on_value=set_user)
        solara.InputText("Token", value=token, on_value=set_token, password=True)
        solara.Button("Scan", style="margin-bottom: 25px", on_click=get_issues, disabled=mounts is None)
        if error is not None and error != "":
            solara.Error("Error: " + error)
        if loading is True:
            solara.Info(f"Loading... Current File: {curr_file} "
                        f"Progress: {progress}/{max_progress} files scanned")
            if max_progress > 0 and progress > 0:
                solara.ProgressLinear((progress / max_progress) * 100)
            else:
                solara.ProgressLinear(True)
        elif issues is not None:
            with solara.Row():
                solara.FileDownload(label="Download Issues Parquet", filename="issues.parquet",
                                    data=lambda: get_raw_data(csv=False))
                solara.FileDownload(label="Download Issues CSV", filename="issues.csv",
                                    data=lambda: get_raw_data(csv=True))
            with solara.lab.Tabs():
                with solara.lab.Tab("Raw Data"):
                    solara.DataFrame(issues)
                if issues.shape[0] > 0:
                    with solara.lab.Tab("Issue Breakdown Pie Chart"):
                        solara.FigurePlotly(get_plotly_mounts())


def make_logger_file_name(timestamp: str = None):
    return f"assessment-{timestamp}.txt"


@solara.component
def Home():
    with solara.Column():
        with solara.lab.Tabs():
            mounts, set_mounts = solara.use_state(cast(Optional[pd.DataFrame], None))
            issues, set_issues = solara.use_state(cast(Optional[List[Issue]], None))
            repo_url, set_repo_url = solara.use_state("")
            user, set_user = solara.use_state("")
            token, set_token = solara.use_state("")
            with solara.lab.Tab(label="Assessments"):
                Assessments()
            with solara.lab.Tab(label="Mount Info"):
                if workspace_conf_ini.value == "":
                    MountScanner(mounts, set_mounts)
                else:
                    # uses multiple workspace clients
                    MountScannerV2(mounts, set_mounts)
            with solara.lab.Tab(label="Repo Scanner"):
                RepoScanner(mounts, set_mounts, issues, set_issues, repo_url,
                            set_repo_url, user, set_user, token, set_token)
            if os.environ.get("EXPERIMENTAL_FIND_AND_REPLACE", "false").lower() == "true":
                with solara.lab.Tab(label="Repo Find and Replace"):
                    CodeFindAndReplace(issues, set_issues, repo_url, set_repo_url, user, set_user, token, set_token)
            with solara.lab.Tab(label="Manage Logs"):
                LogBrowser(LOGS_FOLDER)


@solara.component
def Page():
    # logging is done at hourly rollup
    ts, _ = solara.use_state(datetime.utcnow().strftime("%Y-%m-%d-%H"))
    nav, set_nav = solara.use_state("home")
    log_file_name = make_logger_file_name(ts)
    change_log_filename(log, log_file_name)

    with solara.AppBar():
        solara.AppBarTitle("Databricks Unity Catalog Utilities")
        solara.Button("Home",
                      on_click=lambda: set_nav("home"),
                      outlined=True,
                      style="margin-right: 20px")
        solara.Button("Settings",
                      on_click=lambda: set_nav("settings"),
                      outlined=True,
                      style="margin-right: 20px")

    if nav == "settings":
        Settings()
    elif nav == "home":
        Home()
