import io
import os
import tempfile
import zipfile
from datetime import datetime
from pathlib import Path
from typing import Callable, cast, List, Optional

import pandas as pd
import solara
import solara.lab

from assessment.code_scanner.mounts import mounts_pdf
from assessment.code_scanner.repos import git_repo
from assessment.code_scanner.scan import LocalFSCodeStrategy
from assessment.code_scanner.utils import get_ws_client, get_ws_browser_hostname, change_log_filename, log

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

    def get_issues():
        if repo_url is None or repo_url == "":
            set_error("Please enter a repo url")
            return
        set_error("")
        if user is not None and user != "" and token is not None and token != "":
            built_repo_url = repo_url.replace("https://", f"https://{user}:{token}@")
        else:
            built_repo_url = repo_url
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


def make_logger_file_name(timestamp: str = None):
    return f"assessment-{timestamp}.txt"


@solara.component
def FileBrowser(exec_base_path, exclude_prefixes: List[str] = None):
    file, set_file = solara.use_state(cast(Optional[Path], None))
    path, set_path = solara.use_state(cast(Optional[Path], None))
    file_content, set_file_content = solara.use_state(cast(Optional[str], None))
    # directory, set_directory = solara.use_state(EXECUTION_BASE_PATH)
    EXECUTION_BASE_PATH = Path(exec_base_path).resolve()
    directory = solara.use_reactive(EXECUTION_BASE_PATH)
    message = solara.use_reactive(None)
    MAX_FILE_CT = 10000
    exclude_prefixes = exclude_prefixes or []

    with solara.Column():
        def filter_path(p: Path) -> bool:
            if any([str(p).startswith(prefix) for prefix in exclude_prefixes]):
                return False
            return True

        def protect():
            def check_base_path(value):
                if not str(value).startswith(str(EXECUTION_BASE_PATH)):
                    directory.value = EXECUTION_BASE_PATH
                    message.value = f"Cannot leave root base path {EXECUTION_BASE_PATH}!"
                else:
                    message.value = None

            return directory.subscribe(check_base_path)

        solara.use_effect(protect)
        if message.value:
            error = message.value
        elif path is None:
            error = "You must select a project root!"
        else:
            error = None

        def count_dir():
            count = 0
            for root, dirs, files in os.walk(str(path), topdown=False):
                for _ in files:
                    count += 1
                    if count > MAX_FILE_CT:
                        return count
            return count

        def download_dir():
            if path is not None and path.is_dir():
                import io
                import zipfile
                zip_buffer = io.BytesIO()
                zf = zipfile.ZipFile(zip_buffer, mode='w')

                def remove_prefix(text, prefix):
                    if text.startswith(prefix):
                        return text[len(prefix):]
                    return text

                for root, dirs, files in os.walk(str(path), topdown=False):
                    for name in files:
                        # zf.writestr()
                        this_file = str(os.path.join(root, name))
                        in_zip_name = remove_prefix(this_file, str(path) + "/")
                        with open(this_file, "rb") as f:
                            zf.writestr(in_zip_name, f.read())
                zf.close()
                return zip_buffer

        def last_10000_lines(p):
            with open(p, "r") as f:
                lines = f.readlines()
                return "".join(lines[-10000:])

        def on_path_select(p: Path) -> None:
            if str(p).startswith(str(EXECUTION_BASE_PATH)):
                set_path(p)
                try:
                    set_file_content(last_10000_lines(p))
                except Exception as e:
                    print(f"Error reading file: {e}")
                message.value = None

        def zip_bytes(input_bytes, file_name):
            output_buffer = io.BytesIO()

            with zipfile.ZipFile(output_buffer, 'w', zipfile.ZIP_DEFLATED) as zipf:
                zipf.writestr(file_name, input_bytes)

            compressed_bytes = output_buffer.getvalue()
            return compressed_bytes

        def empty_file(file_path):
            try:
                with open(file_path, 'w') as f:
                    f.truncate(0)
                set_file_content("")
                print(f"Contents of '{file_path}' have been emptied.")
            except Exception as e:
                print(f"Error emptying file '{file_path}': {e}")

        if path is not None and path.is_file():
            solara.Info(f"You selected file for download: {path}")
            # must be lambda otherwise will always try to download
            with solara.HBox():
                solara.FileDownload(lambda: zip_bytes(path.open("rb").read(), path.name), path.name + ".zip",
                                    label=f"Download {path.name}.zip")
                solara.Button(f"Clear Logs {path.name}", on_click=lambda: empty_file(str(path)),
                              style="margin-left: 25px")

        if path is not None and path.is_dir():
            file_ct = count_dir()
            if file_ct >= MAX_FILE_CT:
                solara.Error(f"Too many files in directory unable to offer download ({file_ct} > {MAX_FILE_CT})")
            else:
                solara.Info(f"You selected directory for download as zip: {path}")
                # solara.Button("Download Directory From DBFS", on_click=download_dir)
                zip_name = path.name + ".zip"
                solara.FileDownload(lambda: download_dir(), zip_name, label=f"Download {file_ct} files in "
                                                                            f"{zip_name}")

        with solara.lab.Tabs():
            with solara.lab.Tab("Log File Browser"):
                solara.FileBrowser(
                    directory,
                    filter=filter_path,
                    on_path_select=on_path_select,
                    on_file_open=set_file,
                    can_select=True,
                ).key("file-browser")
            if path is not None:
                with solara.lab.Tab("Log Viewer"):
                    with solara.VBox():
                        with solara.Card(f"Last 10k Logs: {path}"):
                            solara.Button(f"Refresh", on_click=lambda: set_file_content(last_10000_lines(path)),
                                          style="margin-left: 25px; margin-bottom: 25px")
                            solara.Markdown("```" + (file_content or "\n") + "```", style="max-width: 100%; "
                                                                                          "max-height: 500px; "
                                                                                          "overflow: scroll;")


@solara.component
def Page():
    # logging is done at hourly rollup
    ts, _ = solara.use_state(datetime.utcnow().strftime("%Y-%m-%d-%H"))
    log_file_name = make_logger_file_name(ts)
    change_log_filename(log, log_file_name)
    with solara.AppBar():
        solara.AppBarTitle("Databricks Unity Catalog Utilities")
    with solara.Column():
        with solara.lab.Tabs():
            with solara.lab.Tab(label="Mount Info"):
                MountScanner()
            with solara.lab.Tab(label="Repo Scanner"):
                RepoScanner()
            with solara.lab.Tab(label="Manage Logs"):
                FileBrowser("logs")
