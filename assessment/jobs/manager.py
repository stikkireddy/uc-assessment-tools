import abc
from textwrap import dedent
from typing import List, Tuple, Dict, Optional

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, SubmitTask, NotebookTask

from assessment.code_scanner.multi_ws import WorkspaceContextManager
from assessment.code_scanner.utils import log
from assessment.jobs.assets import AssetManager
from assessment.jobs.repository import JobRunRepository, JobRun


class BaseJobsResultsManager(abc.ABC):
    # Goal of this class is to make sure job is executed successfully
    # and then retrieve results of job
    # as a pandas dataframe

    def __init__(self,
                 databricks_ws_clients: List[WorkspaceClient],
                 repository: JobRunRepository,
                 workspace_url_cluster_id_mapping: Dict[str, str]
                 ):
        self._workspace_url_cluster_id_mapping = workspace_url_cluster_id_mapping
        self._ws_clients = databricks_ws_clients
        self._repository = repository

    @abc.abstractmethod
    def _get_results(self, workspace_client: WorkspaceClient, cluster_id: str) -> pd.DataFrame:
        pass

    @abc.abstractmethod
    def job_name(self) -> str:
        pass

    # @abc.abstractmethod
    # def _ensure_artifacts(self, workspace_client: WorkspaceClient) -> None:
    #     pass

    def get_latest_results(self,
                           list_of_workspace_urls: Optional[List[str]] = None) -> pd.DataFrame:
        dfs = []
        list_of_workspace_urls = list_of_workspace_urls or [client.config.host for client in self._ws_clients]
        for client in self._ws_clients:
            if client.config.host in list_of_workspace_urls:
                print("Getting results for workspace: ", client.config.host)
                cluster_id = self._workspace_url_cluster_id_mapping[client.config.host]
                df = self._get_results(client, cluster_id)
                latest_job = self._repository.get_latest_successful_run(client.config.host, self.job_name())
                df["_run_id"] = latest_job.run_id
                df["_run_url"] = latest_job.run_url
                df["_workspace_url"] = latest_job.workspace_url
                df["_lifecycle_state"] = latest_job.lifecycle_state
                df["_result_state"] = latest_job.result_state
                df["_start_time"] = latest_job.start_time
                df["_end_time"] = latest_job.end_time
                dfs.append(df)
        if not dfs:
            return pd.DataFrame()
        return pd.concat(dfs)

    @abc.abstractmethod
    def _create_run(self, workspace_client: WorkspaceClient, cluster_id: str) -> str:
        pass

    def create_runs(self, list_of_workspace_urls: List[Tuple[str, str]], alias_mapping: Dict[str, str]) -> None:
        for client in self._ws_clients:
            if client.config.host in list_of_workspace_urls:
                cluster_id = self._workspace_url_cluster_id_mapping[client.config.host]
                log.info(f"Creating run in workspace: %s on cluster: %s", client.config.host, cluster_id)
                run_id = self._create_run(client, cluster_id)
                run_url = client.jobs.get_run(run_id=run_id).run_page_url
                log.info(f"Created run in workspace: {client.config.host} with run_id: {run_id} and run_url: {run_url}")
                self._repository.create_job_run(
                    JobRun(
                        workspace_url=client.config.host,
                        run_id=run_id,
                        run_url=run_url,
                        workspace_alias=alias_mapping[client.config.host],
                        job_name=self.job_name()
                    )
                )

    def update_run_status(self):
        for client in self._ws_clients:
            runs: List[str] = self._repository.get_incomplete_run_ids(
                workspace_urls=[client.config.host],
            )
            state_updates = []
            for run_id in runs:
                run_status = client.jobs.get_run(run_id=run_id).state
                log.debug(f"Updating run_id: {run_id} with state: {run_status.life_cycle_state}")
                state_updates.append((run_id, run_status.life_cycle_state or RunLifeCycleState.PENDING,
                                      run_status.result_state))

            self._repository.update_job_run_state(state_updates)


# class FakeJob(BaseJobsResultsManager):
#
#     def _ensure_artifacts(self, workspace_client: WorkspaceClient) -> None:
#         return None
#
#     def _get_results(self, workspace_client: WorkspaceClient) -> pd.DataFrame:
#         data = {'col_1': [3, 2, 1, 0], 'col_2': ['a', 'b', 'c', 'd']}
#         return pd.DataFrame.from_dict(data)
#
#     def _create_run(self, workspace_client: WorkspaceClient, cluster_id: str) -> str:
#         res = workspace_client.jobs.run_now(job_id=18780851108171)
#         return res.bind()["run_id"]


class HMSAnalysisJob(BaseJobsResultsManager):

    def job_name(self) -> str:
        return "hms_analysis"

    def _delta_table_path(self):
        return "/tmp/uc_assessment/hms/default.csv"

    def _get_results(self, workspace_client: WorkspaceClient, cluster_id: str) -> pd.DataFrame:
        with WorkspaceContextManager(workspace_client, cluster_id) as ctx:
            return ctx.execute_python(dedent(f"""
            df = spark.read.option("header","true").option("inferSchema","true").csv("{self._delta_table_path()}")
            df.createOrReplaceTempView("tmptable")
            spark.sql('''
                select type, format, count(*) as tableCount
                from tmptable where group by type, format
                order by type, format
            ''').display()
            """), as_pdf=True)
        # data = {'col_1': [3, 2, 1, 0], 'col_2': ['a', 'b', 'c', 'd']}
        # return pd.DataFrame.from_dict(data)

    def _create_run(self, workspace_client: WorkspaceClient, cluster_id: str) -> str:
        path = AssetManager(workspace_client).notebook_path("inspect_metastore.py")
        # res = workspace_client.jobs.run_now(job_id=18780851108171)
        run_wait = workspace_client.jobs.submit(
            run_name=self.job_name(),
            tasks=[
                SubmitTask(
                    task_key="hms_analysis",
                    existing_cluster_id=cluster_id,  # TODO REMOVE
                    notebook_task=NotebookTask(
                        notebook_path=path,
                        base_parameters={
                            "dbfs_file_path": self._delta_table_path(),
                            "debug_mode": "True"
                        }
                    )
                )
            ]
        )
        return run_wait.response.run_id
