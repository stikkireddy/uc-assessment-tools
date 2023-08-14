import tempfile
from pathlib import Path
from typing import Callable

import pandas as pd
import solara
import solara.lab

from assessment.code_scanner.mounts import mounts_pdf
from assessment.code_scanner.repos import git_repo
from assessment.code_scanner.scan import LocalFSCodeStrategy
from assessment.code_scanner.utils import get_ws_client, get_ws_browser_hostname


workspace_url = get_ws_browser_hostname() or get_ws_client(default_profile="uc-assessment-azure").config.host

@solara.component
def MountScanner():
    mounts, set_mounts = solara.use_state(None)
    loading, set_loading = solara.use_state(False)

    mounts: pd.DataFrame
    set_mounts: Callable[[pd.DataFrame], None]

    def get_raw_data(csv=False):
        df_copy = mounts.copy(deep=True)
        df_copy['workspace_url'] = workspace_url
        if csv is True:
            return df_copy.to_csv(index=False)

        return df_copy.to_parquet(index=False)

    def get_mounts():
        set_loading(True)
        mounts_pd = mounts_pdf("abfss")
        set_mounts(mounts_pd)
        set_loading(False)

    with solara.Card("Download Mounts Info"):
        solara.Info("Note: This will ignore mounts: DatabricksRoot, DbfsReserved, UnityCatalogVolumes, "
                    "databricks/mlflow-tracking, databricks-datasets, databricks/mlflow-registry, databricks-results.")
        solara.Button("Load Mounts", on_click=get_mounts, style="margin-bottom: 25px")
        if loading is True:
            solara.Info(f"Loading...")
            solara.ProgressLinear(True)
        elif mounts is not None:
            solara.FileDownload(label="Download Mounts Info", filename="mounts.csv",
                                data=lambda: get_raw_data(csv=True))
            solara.DataFrame(mounts)


@solara.component
def RepoScanner():
    issues, set_issues = solara.use_state(None)
    loading, set_loading = solara.use_state(False)
    error, set_error = solara.use_state("")
    repo_url, set_repo_url = solara.use_state("")
    user, set_user = solara.use_state("")
    token, set_token = solara.use_state("")

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
        # grouped_counts = issues.groupby(['issue_type', 'issue_detail']).size().reset_index(name='count')
        #
        # # Create a pie chart using Plotly Express
        # return px.pie(grouped_counts, values='count', names='issue_detail', title="Issue Type and Detail Breakdown",
        #              color='issue_type')

    def get_issues():
        if repo_url is None or repo_url == "":
            set_error("Please enter a repo url")
            return
        if user is None or user == "":
            set_error("Please enter a user")
            return
        if token is None or token == "":
            set_error("Please enter a token")
            return
        set_error("")
        built_repo_url = repo_url.replace("https://", f"https://{user}:{token}@")
        try:
            set_loading(True)
            ws_client = get_ws_client(default_profile="uc-assessment-azure")
            curr_user = ws_client.current_user.me()
            user_name = curr_user.display_name
            email = curr_user.user_name
            with tempfile.TemporaryDirectory() as path:
                with git_repo(built_repo_url, None, path, email=email, full_name=user_name, delete=True):
                    print("Inside the context manager")
                    scan = LocalFSCodeStrategy([Path(path)])
                    # this identifies all the issues in the repo
                    set_issues(scan.to_df())
        except Exception as e:
            set_error(str(e))
        finally:
            set_loading(False)

    with solara.Card("Scan Mounts in Repos"):
        solara.Info("Note: This will ignore mounts: DatabricksRoot, DbfsReserved, UnityCatalogVolumes, "
                    "databricks/mlflow-tracking, databricks-datasets, databricks/mlflow-registry, databricks-results.")
        solara.InputText("Repo Url", value=repo_url, on_value=set_repo_url)
        solara.InputText("User Name", value=user, on_value=set_user)
        solara.InputText("Token", value=token, on_value=set_token, password=True)
        solara.Button("Scan", style="margin-bottom: 25px", on_click=get_issues)
        if error is not None and error != "":
            solara.Error("Error: " + error)
        if loading is True:
            solara.Info(f"Loading...")
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
                with solara.lab.Tab("Issue Breakdown Pie Chart"):
                    solara.FigurePlotly(get_plotly_mounts())


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
