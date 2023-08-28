import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import List, cast, Type

import pandas as pd
import solara
import solara.lab

from assessment.code_scanner.utils import get_db_base_path, zip_bytes
from assessment.jobs.assets import AssetManager
from assessment.jobs.manager import HMSAnalysisJob, JobRunResults, BaseJobsResultsManager
from assessment.jobs.repository import JobRunRepository, JobRun
from assessment.ui.components.valid_client_checklist import ValidClientCheckList
from assessment.ui.models import WorkspaceConf
from assessment.ui.state import workspace_conf_ini


@dataclass
class AssessmentRow:
    workspace_name: str
    assessment_name: str
    assessment_lifecycle_status: str
    assessment_result_state: str
    assessment_link: str


@solara.component
def Space():
    solara.HTML(unsafe_innerHTML='<div style="margin-top: 10px;"></div>')


def dataframe_modified(df, optional_columns=None,
                       drop_columns=None, copy_deep=True
                       ):
    # Convert HTML columns to Markdown link fields using link_mapping
    new_df = df
    if copy_deep is True:
        new_df = df.copy(deep=True)

    # Filter columns based on optional_columns
    if optional_columns:
        new_df = new_df[['index'] + optional_columns]
    if drop_columns:
        new_df = new_df.drop(columns=drop_columns)

    new_df['start_time'] = pd.to_datetime(new_df['start_time'])  # Convert start_time to datetime

    # Sort and add last_n_runs column
    new_df = new_df.sort_values(by=['workspace_url', 'start_time'], ascending=[True, False])
    new_df['last_n_runs'] = new_df.groupby('workspace_url').cumcount() + 1

    return new_df


def get_clients_urls_clusters(selected_ws: solara.Reactive[List[str]]):
    wc = WorkspaceConf.from_ini(workspace_conf_ini.value)
    clients = [config.get_ws_client() for key, config in wc.configs.items() if key in selected_ws.value]
    urls = [config.get_ws_client().config.host for key, config in wc.configs.items() if key in selected_ws.value]
    cluster_dict = {config.get_ws_client().config.host: config.cluster_id
                    for key, config in wc.configs.items() if key in selected_ws.value}
    workspace_alias_mapping = {config.get_ws_client().config.host: key
                               for key, config in wc.configs.items() if key in selected_ws.value}
    return clients, urls, cluster_dict, workspace_alias_mapping


db_base_path = Path(get_db_base_path()).parent


@solara.component
def AssessmentBlock(assessment_name: str, selected_ws: solara.Reactive[List[str]],
                    manager_klass: Type[BaseJobsResultsManager]):
    # assessment_rows, set_assessment_rows = solara.use_state(cast(List[AssessmentRow], []))
    run_history, set_run_history = solara.use_state(cast(pd.DataFrame, None))
    latest_runs, set_latest_runs = solara.use_state(cast(pd.DataFrame, None))
    loading, set_loading = solara.use_state(False)
    results_loading, set_results_loading = solara.use_state(False)
    assessment_loading, set_assessment_loading = solara.use_state(False)
    run_history_msg, set_run_history_msg = solara.use_state("")
    runs_results, set_runs_results = solara.use_state(cast(List[JobRunResults], None))

    def get_assessment_rows():
        clients, urls, cluster_dict, _ = get_clients_urls_clusters(selected_ws)
        repo = JobRunRepository(db_base_path / "test.db")
        manager = manager_klass(clients, repo, cluster_dict)
        while True:
            set_loading(True)
            set_run_history(JobRun.to_dataframe(
                repo.get_latest_run_results(urls,
                                            [manager.job_name()],
                                            100),
                json_friendly=True,
                no_millis=True
            ))
            set_latest_runs(
                JobRun.to_dataframe(
                    repo.get_latest_run_results(urls,
                                                [manager.job_name()],
                                                1),
                    json_friendly=True,
                    no_millis=True
                )
            )
            set_loading(False)
            time.sleep(5)

    def submit_assessment():
        set_assessment_loading(True)
        clients, urls, cluster_dict, workspace_alias_mapping = get_clients_urls_clusters(selected_ws)
        repo = JobRunRepository(db_base_path / "test.db")
        manager = manager_klass(clients, repo, cluster_dict)
        manager.create_runs(urls, workspace_alias_mapping)
        set_assessment_loading(False)

    def update_assessment():
        while True:
            set_loading(True)
            set_run_history_msg(f"Updating assessment status... last refreshed: {datetime.utcnow()}")
            clients, urls, cluster_dict, _ = get_clients_urls_clusters(selected_ws)
            repo = JobRunRepository(db_base_path / "test.db")
            manager = manager_klass(clients, repo, cluster_dict)
            manager.update_run_status()
            set_loading(False)
            time.sleep(5)

    def get_run_results():
        clients, urls, cluster_dict, _ = get_clients_urls_clusters(selected_ws)
        set_results_loading(True)
        repo = JobRunRepository(db_base_path / "test.db")
        manager = manager_klass(clients, repo, cluster_dict)
        results = [result for result in manager.get_latest_results(None)]
        set_runs_results(results)
        set_results_loading(False)

    solara.use_thread(get_assessment_rows, [selected_ws.value])
    solara.use_thread(update_assessment, [])

    with solara.Details("", expand=True):
        ValidClientCheckList(selected_ws, clusters=True)
        solara.Button(f"Run {assessment_name} on selected workspaces",
                      icon_name="play_arrow", on_click=submit_assessment)

        if assessment_loading is True:
            Space()
            solara.Info(f"Running {assessment_name} on selected workspaces...")
            solara.ProgressLinear(True)
        Space()
        with solara.lab.Tabs():
            with solara.lab.Tab("Latest Runs"):
                if latest_runs is not None and latest_runs.empty is False:
                    solara.HTML(unsafe_innerHTML='<div style="margin-top: 10px;"></div>')
                    if run_history_msg != "":
                        solara.Info(run_history_msg, style="margin-top: 10px;")
                    solara.provide_cross_filter()
                    with solara.VBox():
                        df = dataframe_modified(latest_runs, drop_columns=["id"])
                        solara.DataFrame(df)
                else:
                    solara.Info("No runs found.", style="margin-top: 10px;")
            with solara.lab.Tab("Run History"):
                if run_history is not None and run_history.empty is False:
                    solara.HTML(unsafe_innerHTML='<div style="margin-top: 10px;"></div>')
                    if run_history_msg != "":
                        solara.Info(run_history_msg, style="margin-top: 10px;")
                    solara.provide_cross_filter()
                    with solara.VBox():
                        df = dataframe_modified(run_history, drop_columns=["id"])
                        solara.DataFrame(df, items_per_page=20)
                else:
                    solara.Info("No runs found.", style="margin-top: 10px;")
            with solara.lab.Tab("Run Results"):
                solara.HTML(unsafe_innerHTML='<div style="margin-top: 10px;"></div>')
                if not selected_ws.value:
                    solara.Error("Please select register and select workspaces to view results.")
                else:
                    solara.Button("Retrieve results...",
                                  style="margin-top: 10px; margin-bottom: 10px;",
                                  icon_name="cloud_download",
                                  on_click=get_run_results)
                if results_loading is True:
                    solara.Info("Loading...")
                    solara.ProgressLinear(True)

                if runs_results is not None:
                    for run_result in runs_results:
                        with solara.Card(run_result.name):
                            solara.FileDownload(
                                data=lambda: zip_bytes(run_result.data.to_csv(), f"{run_result.name}.zip"),
                                filename=f"{run_result.name}.csv")
                            solara.DataFrame(run_result.data)


def AssetsBlock(selected_ws: solara.Reactive[List[str]]):
    loading, set_loading = solara.use_state(False)
    workspace_url, set_workspace_url = solara.use_state("")

    with solara.Details("", expand=True):
        ValidClientCheckList(selected_ws, clusters=True)

        def upload_all_assets():
            set_loading(True)
            clients, _, _, _ = get_clients_urls_clusters(selected_ws)
            for client in clients:
                set_workspace_url(client.config.host)
                AssetManager(client).upload_all()
            set_workspace_url("")
            set_loading(False)

        solara.Button("Upload All Assets", icon_name="cloud_upload", on_click=upload_all_assets)
        if loading is True:
            solara.Info(f"Uploading all assets... to {workspace_url}")
            solara.ProgressLinear(True)


@solara.component
def Assessments():
    selected_ws: solara.Reactive[List[str]] = solara.use_reactive([])
    with solara.Card("Assessments"):
        with solara.Card("Assets"):
            AssetsBlock(selected_ws)
        with solara.Card("HMS Assessment"):
            AssessmentBlock("HMS Assessment", selected_ws, HMSAnalysisJob)
