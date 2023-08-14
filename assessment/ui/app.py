import tempfile
from pathlib import Path
from typing import Callable

import pandas as pd
import solara
import solara.lab

from assessment.code_scanner.mounts import mounts_pdf
from assessment.code_scanner.repos import git_repo
from assessment.code_scanner.scan import get_dbutils, LocalFSCodeStrategy
from assessment.code_scanner.utils import get_ws_client


@solara.component
def MountScanner():
    mounts, set_mounts = solara.use_state(None)
    loading, set_loading = solara.use_state(False)

    mounts: pd.DataFrame
    set_mounts: Callable[[pd.DataFrame], None]

    def get_mounts():
        set_loading(True)
        mounts_pd = mounts_pdf(get_dbutils(), "abfss")
        set_mounts(mounts_pd)
        set_loading(False)

    with solara.Card("Download Mounts Info"):
        solara.Button("Load Mounts", on_click=get_mounts, style="margin-bottom: 25px")
        if loading is True:
            solara.Info(f"Loading...")
            solara.ProgressLinear(True)
        elif mounts is not None:
            solara.FileDownload(label="Download Mounts Info", filename="mounts.csv",
                                data=lambda: mounts.to_csv(index=False))
            solara.DataFrame(mounts)


@solara.component
def RepoScanner():
    issues, set_issues = solara.use_state(None)
    loading, set_loading = solara.use_state(False)
    error, set_error = solara.use_state("")
    repo_url, set_repo_url = solara.use_state("")

    issues: pd.DataFrame
    set_issues: Callable[[pd.DataFrame], None]

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
                with git_repo(repo_url, None, path, email=email, full_name=user_name, delete=True):
                    print("Inside the context manager")
                    scan = LocalFSCodeStrategy([Path(path)])
                    # this identifies all the issues in the repo
                    set_issues(scan.to_df())
        except Exception as e:
            print(e)
            set_error(str(e))
        finally:
            set_loading(False)

    with solara.Card("Scan Mounts in Repos"):
        solara.InputText("Repo Path", value=repo_url, on_value=set_repo_url)
        solara.Button("Scan", style="margin-bottom: 25px", on_click=get_issues)
        if error:
            solara.Error("Error: " + error)
        if loading is True:
            solara.Info(f"Loading...")
            solara.ProgressLinear(True)
        elif issues is not None:
            solara.FileDownload(label="Download Issues", filename="issues.csv",
                                data=lambda: issues.to_csv(index=False))
            solara.DataFrame(issues)


@solara.component
def Page():
    with solara.AppBar():
        solara.AppBarTitle("Databricks Unity Catalog Utilities")
    with solara.Column():
        with solara.lab.Tabs():
            with solara.lab.Tab(label="Mount Info"):
                MountScanner()
            with solara.lab.Tab(label="Repo Scanner"):
                RepoScanner()
