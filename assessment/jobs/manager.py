import abc
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path
from textwrap import dedent
from typing import List, Tuple, Dict, Optional, Iterator

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.jobs import RunLifeCycleState, SubmitTask, NotebookTask

from assessment.code_scanner.multi_ws import WorkspaceContextManager
from assessment.code_scanner.utils import log
from assessment.jobs.assets import AssetManager
from assessment.jobs.repository import JobRunRepository, JobRun


@dataclass
class JobRunResults:
    name: str
    data: pd.DataFrame
    description: str
    code: Optional[str] = None


class BaseJobsResultsManager(abc.ABC):
    # Goal of this class is to make sure job is executed successfully
    # and then retrieve results of job
    # as a pandas dataframe

    def __init__(self,
                 databricks_ws_clients: List[WorkspaceClient],
                 repository: JobRunRepository,
                 workspace_url_cluster_id_mapping: Dict[str, str],
                 workspace_alias_mapping: Dict[str, str] = None
                 ):
        self._workspace_alias_mapping = workspace_alias_mapping or {}
        self._workspace_url_cluster_id_mapping = workspace_url_cluster_id_mapping
        self._ws_clients = databricks_ws_clients
        self._repository = repository

    @abc.abstractmethod
    def _get_results(self, workspace_client: WorkspaceClient, cluster_id: str) -> Iterator[JobRunResults]:
        pass

    @abc.abstractmethod
    def job_name(self) -> str:
        pass

    def get_latest_results(self,
                           list_of_workspace_urls: Optional[List[str]] = None) -> Iterator[JobRunResults]:
        results = defaultdict(list)
        list_of_workspace_urls = list_of_workspace_urls or [client.config.host for client in self._ws_clients]
        for client in self._ws_clients:
            if client.config.host in list_of_workspace_urls:
                cluster_id = self._workspace_url_cluster_id_mapping[client.config.host]
                latest_job = self._repository.get_latest_successful_run(client.config.host, self.job_name())
                for res in self._get_results(client, cluster_id):
                    df = res.data
                    # latest job can be null if the database is deleted
                    if latest_job is not None:
                        df["_run_id"] = latest_job.run_id
                        df["_run_url"] = latest_job.run_url
                        df["_workspace_url"] = latest_job.workspace_url
                        df["_lifecycle_state"] = latest_job.lifecycle_state
                        df["_result_state"] = latest_job.result_state
                        df["_start_time"] = latest_job.start_time
                        df["_end_time"] = latest_job.end_time
                    results[res.name].append(res)

        for name, results_list in results.items():
            if len(results_list) >= 1:
                yield JobRunResults(name=name,
                                    data=pd.concat([res.data for res in results_list]),
                                    description=results_list[0].description)
            else:
                yield JobRunResults(name=name, data=pd.DataFrame(), description="")

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
                log.info(f"Updating run_id: {run_id} with state: {run_status.life_cycle_state}")
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

    def _get_results(self, workspace_client: WorkspaceClient, cluster_id: str) -> Iterator[JobRunResults]:
        with WorkspaceContextManager(workspace_client, cluster_id) as ctx:
            df = ctx.execute_python(dedent(f"""
            df = spark.read.option("header","true").option("inferSchema","true").csv("{self._delta_table_path()}")
            df.createOrReplaceTempView("tmptable")
            spark.sql('''
                select type, format, count(*) as tableCount
                from tmptable where group by type, format
                order by type, format
            ''').display()
            """), as_pdf=True)
            yield JobRunResults(name="hms_analysis", data=df, description="HMS Analysis for Table Types in HMS")

    def _create_run(self, workspace_client: WorkspaceClient, cluster_id: str) -> str:
        path = AssetManager(workspace_client).notebook_path("inspect_metastore.py")
        run_wait = workspace_client.jobs.submit(
            run_name=self.job_name(),
            tasks=[
                SubmitTask(
                    task_key=f"{self.job_name()}_task",
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


class ComputeAnalysisJob(BaseJobsResultsManager):

    def job_name(self) -> str:
        return "compute_analysis"

    def _delta_table_base_path(self):
        # needs to start with dbfs according to the job
        return "dbfs:/tmp/uc_assessment/compute/"

    def _final_analysis_table(self) -> str:
        return str(Path(self._delta_table_base_path()) / "analysis/compute_issues/delta")

    def _final_jobs_table(self) -> str:
        return str(Path(self._delta_table_base_path()) / "jobs/delta")

    def _final_job_runs_table(self) -> str:
        return str(Path(self._delta_table_base_path()) / "job_submit_runs/delta")

    def _get_results(self, workspace_client: WorkspaceClient, cluster_id: str) -> Iterator[JobRunResults]:
        with WorkspaceContextManager(workspace_client, cluster_id) as ctx:
            df = ctx.execute_python(dedent(f"""
            spark.sql('''
                SELECT _workspace_url, entity_type, "TOTAL_ENTITIES" as metric_type, count(DISTINCT entity_id) as metric_value
                FROM
                  (SELECT "WORKFLOW" as entity_type, job_id as entity_id, _workspace_url FROM delta.`{self._final_jobs_table()}` UNION ALL
                  SELECT "SUBMIT_RUN" as entity_type, run_id as entity_id, _workspace_url FROM delta.`{self._final_job_runs_table()}`)
                  GROUP BY 1, 2, 3
                UNION ALL
                SELECT workspace_url, entity_type, "TOTAL_ENTITIES_WITH_ISSUES" as metric_type, count(DISTINCT entity_id) as metric_value
                FROM delta.`{self._final_analysis_table()}`
                WHERE contains(entity_issue_type, "NOT_AN_ISSUE_") is false
                GROUP BY 1, 2, 3
            ''').display()
            """), as_pdf=True)
            yield JobRunResults(name="workflow_issue_summary", data=df,
                                description="Workflow Analysis for workflows with issues and workflows without")

            df = ctx.execute_python(dedent(f"""
                spark.sql('''
                    SELECT workspace_url, entity_type, entity_issue_type, entity_issue_detail, sum(issue_ct) as issue_ct
                FROM
                (SELECT workspace_url, entity_type,entity_id, entity_issue_type, entity_issue_detail, count(1) as issue_ct 
                    FROM delta.`{self._final_analysis_table()}`
                GROUP BY 1, 2, 3, 4, 5)
                GROUP BY 1, 2, 3, 4
                ORDER BY 5 desc
                ''').display()
            """), as_pdf=True)
            yield JobRunResults(name="issue_summary", data=df,
                                description="List of all issues and count of issues.")

    def _create_run(self, workspace_client: WorkspaceClient, cluster_id: str) -> str:
        path = AssetManager(workspace_client).notebook_path("inspect_compute.py")
        run_wait = workspace_client.jobs.submit(
            run_name=self.job_name(),
            tasks=[
                SubmitTask(
                    task_key=f"{self.job_name()}_task",
                    existing_cluster_id=cluster_id,  # TODO REMOVE
                    notebook_task=NotebookTask(
                        notebook_path=path,
                        base_parameters={
                            "dbfs_folder_path": self._delta_table_base_path(),
                            "debug_mode": "True",
                            "workspace_name": self._workspace_alias_mapping.get(workspace_client.config.host,
                                                                                "unidentified"),
                            "workspace_url": workspace_client.config.host
                        }
                    )
                )
            ]
        )
        return run_wait.response.run_id
