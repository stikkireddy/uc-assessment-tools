# Databricks notebook source
# MAGIC %pip install databricks-sdk>=0.4.0 --force-reinstall

# COMMAND ----------

# MAGIC %pip list

# COMMAND ----------

dbutils.library.restartPython()

# COMMAND ----------

from databricks.sdk import WorkspaceClient

client = WorkspaceClient()

# COMMAND ----------

# DBTITLE 1,Initialize Base Variables
dbutils.widgets.text("dbfs_folder_path", "dbfs:/tmp/uc_assessment/compute/", label="File Path")
dbutils.widgets.text("workspace_name", "", label="Workspace Name")
dbutils.widgets.text("workspace_url", "", label="Workspace Url")
dbutils.widgets.text("debug_mode", "True", label="Debug Mode")

# COMMAND ----------

from pathlib import Path

dbfs_folder_path = Path(dbutils.widgets.get("dbfs_folder_path"))
workspace_name = dbutils.widgets.get("workspace_name")
workspace_url = dbutils.widgets.get("workspace_url")
debug_mode = dbutils.widgets.get("debug_mode") == "True"
dbfs_folder_path, debug_mode, workspace_name, workspace_url

# COMMAND ----------

# DBTITLE 1,Commit Manager to incrementally commit data to table
import os
import typing
from dataclasses import dataclass
from typing import Optional, Dict, Any, List

from delta import DeltaTable
from pyspark.sql import SparkSession

from pyspark.sql.types import StructField, LongType, StringType, StructType
from urllib3 import Retry


def is_optional(field):
    return typing.get_origin(field) is typing.Union and \
        type(None) in typing.get_args(field)


@dataclass
class BaseData:

    @staticmethod
    def raw_data_key():
        return "_raw_data"

    @staticmethod
    def workspace_name_key():
        return "_workspace_name"

    @staticmethod
    def workspace_url_key():
        return "_workspace_url"

    @classmethod
    def to_struct_type(cls):
        fields = []
        for k, v in cls.__dict__["__annotations__"].items():
            if v in [int, Optional[int]]:
                fields.append(StructField(name=k, dataType=LongType()))
            elif v in [str, Optional[str]]:
                fields.append(StructField(name=k, dataType=StringType()))
            else:
                raise Exception(f"not supported key: {k} data type {type(v)} for class: {cls.__name__}")
        fields.append(StructField(name=cls.raw_data_key(), dataType=StringType()))
        fields.append(StructField(name=cls.workspace_name_key(), dataType=StringType()))
        fields.append(StructField(name=cls.workspace_url_key(), dataType=StringType()))
        return StructType(fields=fields)

    @classmethod
    def from_api_to_dict(cls, api_json: Dict[str, Any], workspace_name: str, workspace_url: str):
        result_data: Dict[str, Any] = {}
        for k, v in cls.__dict__["__annotations__"].items():
            if k != "raw_data":
                result_data[k] = api_json.get(k, None)
        result_data[cls.raw_data_key()] = json.dumps(api_json)
        result_data[cls.workspace_name_key()] = workspace_name
        result_data[cls.workspace_url_key()] = workspace_url
        return result_data


def get_retry_class(max_retries):
    class LogRetry(Retry):
        """
           Adding extra logs before making a retry request
        """

        def __init__(self, *args, **kwargs):
            if kwargs.get("total", None) != max_retries and kwargs.get("total", None) > 0:
                print(f'Retrying with kwargs: {kwargs}')
            super().__init__(*args, **kwargs)

    return LogRetry


def debug(*args):
    if os.getenv("DATABRICKS_EXPORT_DEBUG", None) is not None:
        print(*args)


class ExportBufferManager:
    # todo: mapping the primary keys
    def __init__(self, name: str, spark: SparkSession, target_table: DeltaTable, primary_keys: List[str],
                 max_buffer_size=10000,
                 debug_every_n_records=1000,
                 auto_flush=True):
        # TODO: not multi threaded dont try to use this multi threaded no locks
        self._name = name
        self._spark = spark
        self._target_table = target_table
        self._max_buffer_size = max_buffer_size
        self._buffer = []
        self._auto_flush = auto_flush
        self._primary_keys = primary_keys
        self._buffer_use_count = 0
        self._debug_every_n_records = debug_every_n_records

    def add_many(self, *elements):
        for element in elements:
            self.add_one(element)

    def _check_commit(self):
        if self._auto_flush is True and len(self._buffer) >= self._max_buffer_size:
            self.commit()

    def commit(self):
        print(f"Committing to delta table: ct {self._buffer_use_count}")
        # TODO: error handling
        input_data = self._spark.createDataFrame(self._buffer, self._target_table.toDF().schema).alias("s") \
            .drop_duplicates(self._primary_keys)
        (self._target_table.alias("t")
         .merge(input_data,
                " and ".join([f"t.{k} = s.{k}" for k in self._primary_keys]))
         .whenMatchedUpdateAll()
         .whenNotMatchedInsertAll()
         .execute())
        # empty the buffer
        self._buffer = []

    def add_one(self, element):
        self._check_commit()
        self._buffer.append(element)
        if self._buffer_use_count % self._debug_every_n_records == 0:
            print(f"Debug [{self._name}]: ct {self._buffer_use_count} / {self._max_buffer_size}")
        self._buffer_use_count += 1

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.commit()
        print(f"Optimizing Table: {self._name}")
        self._target_table.optimize().executeCompaction()


# COMMAND ----------

# DBTITLE 1,Handle Workflow Configurations and Job Submit Runs (ADF, Airflow, etc)
@dataclass
class JobData(BaseData):
    job_id: int
    name: str
    creator_user_name: str


@dataclass
class JobRunData(BaseData):
    job_id: int
    run_id: int
    creator_user_name: str
    start_time: int
    setup_duration: int
    cleanup_duration: int
    end_time: int
    run_duration: int
    trigger: str
    run_name: str
    run_page_url: str
    run_page_url: str
    attempt_number: Optional[str]


class JobRunsHandler:

    def __init__(self,
                 spark: SparkSession,
                 target_table_location: str,
                 client: WorkspaceClient,
                 workspace_url: str,
                 workspace_name: str = "undefined",
                 last_n_days: int = 7,
                 buffer_size=10000):
        self._spark = spark
        self._target_table_location = target_table_location
        self._last_n_days = last_n_days
        self._workspace_name = workspace_name
        self._host = workspace_url
        self._buffer_size = buffer_size

    def create_table(self):
        # TODO: replace with delta table builder
        self._spark.createDataFrame([], JobRunData.to_struct_type()) \
            .write \
            .format("delta") \
            .option("mergeSchema", "true") \
            .save(self._target_table_location)

    def job_runs_iter(self):
        for run in client.jobs.list_runs(completed_only="true", run_type=jobs.ListRunsRunType.SUBMIT_RUN,
                                         expand_tasks="true"):
            yield run.as_dict()

    def run(self):
        if DeltaTable.isDeltaTable(self._spark, self._target_table_location) is False:
            self.create_table()
        tgt = DeltaTable.forPath(self._spark, self._target_table_location)
        with ExportBufferManager("Job Runs Buffer", self._spark, tgt,
                                 primary_keys=["job_id", "run_id", JobRunData.workspace_url_key()],
                                 max_buffer_size=self._buffer_size) as buf:
            for r in self.job_runs_iter():
                data = JobRunData.from_api_to_dict(r, self._workspace_name, self._host.rstrip("/"))
                buf.add_one(data)  # buffers n records and merges into


class JobsHandler:

    def __init__(self,
                 spark: SparkSession,
                 target_table_location: str,
                 client: WorkspaceClient,
                 workspace_url: str,
                 workspace_name: str = "undefined",
                 buffer_size=2000):
        self._spark = spark
        self._target_table_location = target_table_location
        self._workspace_name = workspace_name
        self._host = workspace_url
        self._buffer_size = buffer_size

    def create_table(self):
        # TODO: replace with delta table builder
        self._spark.createDataFrame([], JobData.to_struct_type()) \
            .write \
            .format("delta") \
            .option("mergeSchema", "true") \
            .save(self._target_table_location)

    def jobs_iter(self):
        for job in client.jobs.list(expand_tasks="true"):
            data = job.as_dict()
            data["name"] = data["settings"]["name"]
            yield data

    def run(self):
        if DeltaTable.isDeltaTable(self._spark, self._target_table_location) is False:
            self.create_table()
        tgt = DeltaTable.forPath(self._spark, self._target_table_location)
        with ExportBufferManager("Job Definition Buffer", self._spark, tgt,
                                 primary_keys=["job_id", JobData.workspace_url_key()],
                                 max_buffer_size=self._buffer_size) as buf:
            for r in self.jobs_iter():
                data = JobData.from_api_to_dict(r, self._workspace_name, self._host.rstrip("/"))
                buf.add_one(data)  # buffers n records and merges into


# COMMAND ----------

# DBTITLE 1,Capture Cluster Data
import functools
from dataclasses import dataclass
from typing import List

from delta import DeltaTable
from pyspark.sql import SparkSession

from databricks.sdk.service import compute


@dataclass
class ClusterDetails(BaseData):
    cluster_source: str
    cluster_id: str


class ClusterDetailsHandler:

    def __init__(self,
                 spark: SparkSession,
                 target_table_location: str,
                 client: WorkspaceClient,
                 cluster_ids: List[str],
                 workspace_url: str,
                 workspace_name: str = "undefined",
                 buffer_size=300):
        self._spark = spark
        self._client = client
        self._target_table_location = target_table_location
        self._cluster_ids = cluster_ids
        self._host = workspace_url
        self._workspace_name = workspace_name
        self._buffer_size = buffer_size

    def create_table(self):
        self._spark.createDataFrame([], ClusterDetails.to_struct_type()) \
            .write \
            .format("delta") \
            .option("mergeSchema", "true") \
            .save(self._target_table_location)

    @functools.lru_cache(maxsize=512)
    def get_cluster_info(self, cluster_id) -> compute.ClusterDetails:
        return self._client.clusters.get(cluster_id=cluster_id)

    def does_cluster_exist(self, cluster_id):
        try:
            cluster_details = self.get_cluster_info(cluster_id)
            return True
        except Exception as e:
            return False

    def run(self):
        if DeltaTable.isDeltaTable(self._spark, self._target_table_location) is False:
            self.create_table()
        tgt = DeltaTable.forPath(self._spark, self._target_table_location)
        with ExportBufferManager("Cluster Details Buffer", self._spark, tgt,
                                 ["cluster_source", "cluster_id",
                                  ClusterDetails.workspace_url_key()], max_buffer_size=self._buffer_size) as buf:
            for cluster_id in self._cluster_ids:
                if self.does_cluster_exist(cluster_id) is False:
                    debug(f"Cluster: {cluster_id} is most probably missing.")
                    continue
                cluster_details = self.get_cluster_info(cluster_id)
                data = ClusterDetails.from_api_to_dict(cluster_details.as_dict(),
                                                       self._workspace_name,
                                                       self._host.rstrip("/"))
                buf.add_one(data)  # buffers n records and merges into


# COMMAND ----------

# DBTITLE 1,ETL To Jobs/Workflows Table
workflows_table_location = str(dbfs_folder_path / "jobs/delta")
JobsHandler(
    spark,
    workflows_table_location,
    client=client,
    workspace_name=workspace_name,
    workspace_url=workspace_url,
    buffer_size=2000  # commit every 2000 workflows retrieved
).run()

# COMMAND ----------

print(workflows_table_location)

# COMMAND ----------

# DBTITLE 1,ETL To Jobs Submit Runs Table
workflow_submit_runs_table_location = str(dbfs_folder_path / "job_submit_runs/delta")
JobRunsHandler(
    spark,
    workflow_submit_runs_table_location,
    client=client,
    last_n_days=1 if debug_mode is True else 60,
    workspace_name=workspace_name,
    workspace_url=workspace_url,
    buffer_size=2000  # commit every 2000 submit runs retrieved
).run()

# COMMAND ----------

print(workflow_submit_runs_table_location)

# COMMAND ----------

import itertools

import pandas as pd

from databricks.sdk.service import jobs
import json
from typing import Optional, Union, Literal, Iterator, Tuple
from dataclasses import dataclass, asdict
from datetime import datetime


@dataclass
class FlaggedIssue:
    entity_type: Literal["WORKFLOW", "SUBMIT_RUN"]
    entity_id: str
    entity_name: str
    entity_issue_type: Literal["RUNTIME_ISSUE", "UNSUPPORTED_DBFS_INIT_SCRIPTS", "DBFS_USAGE_FOUND"]
    entity_issue_detail: Literal[
        "DBFS_USAGE_FOUND"
        "UNSUPPORTED_RUNTIME",
        "UNSUPPORTED_RUNTIME_CUSTOM_IMAGE",
        "UNSUPPORTED_DBFS_INIT_SCRIPTS"]
    entity_issue_key: str
    entity_issue_value: str
    entity_url: Optional[str]
    workspace_url: Optional[str]
    workspace_id: Optional[str] = None
    entity_create_ts_utc: Optional[str] = None


@dataclass
class ClusterEntity:
    entity_type: Literal["WORKFLOW", "SUBMIT_RUN"]
    entity_id: str
    entity_name: str
    entity_cluster_id: str
    entity_cluster_field: str
    entity_url: Optional[str]
    workspace_url: Optional[str]
    workspace_id: Optional[str] = None
    entity_create_ts_utc: Optional[str] = None


def get_if_job(data: dict) -> Optional[jobs.Job]:
    try:
        return jobs.Job.from_dict(data)
    except Exception:
        return None


def get_if_submit_run(data: dict) -> Optional[jobs.BaseRun]:
    try:
        return jobs.BaseRun.from_dict(data)
    except Exception as e:
        print("failed submit run")
        return None


def get_job_url(job: jobs.Job, workspace_url: str) -> str:
    workspace_url = workspace_url.rstrip("/")
    return f"{workspace_url}/#job/{job.job_id}"


def generate_key_value_pairs(data, parent_key=''):
    if isinstance(data, dict):
        for key, value in data.items():
            new_key = f"{parent_key}.{key}" if parent_key else key
            yield from generate_key_value_pairs(value, new_key)
    elif isinstance(data, list):
        for index, value in enumerate(data):
            new_key = f"{parent_key}[{index}]"
            yield from generate_key_value_pairs(value, new_key)
    else:
        yield parent_key, data


def find_runtime_issue(k: str, v: Union[str, int, float, bool]) -> Iterator[Optional[Tuple[str, str]]]:
    if k.endswith("spark_version"):
        if v.startswith("custom"):
            yield "RUNTIME_ISSUE", "UNSUPPORTED_RUNTIME_CUSTOM_IMAGE"
        else:
            try:
                if "-ml-" in v:
                    yield "RUNTIME_ISSUE", "UNSUPPORTED_RUNTIME_FOR_SHARED_ML"
                major_version = v.split(".")[0]
                minor_version = v.split(".")[1]
                version = float(major_version + "." + minor_version)
                if version < 11.3:
                    yield "RUNTIME_ISSUE", f"UNSUPPORTED_RUNTIME_VERSION_{version}"
                elif version < 13.3:
                    yield "POTENTIAL_RUNTIME_ISSUE", f"SUPPORTED_RUNTIME_BUT_SUBOPTIMAL_VERSION_{version}"
            except Exception:
                yield "POTENTIAL_RUNTIME_ISSUE", f"UNKNOWN_RUNTIME_ISSUE_{str(v)}"


def find_dbfs_issue(k: str, v: Union[str, int, float, bool]) -> Iterator[Optional[Tuple[str, str]]]:
    # return issue_type and issue_detail
    if k == "" or not isinstance(v, str):
        return

    if k != "" and isinstance(v, str) and ("dbfs:/mnt/" in v or "/mnt/" in v):
        yield "DBFS_MNT_USAGE_FOUND", "DBFS_MNT_USAGE_FOUND"
    elif k != "" and isinstance(v, str) and ("/dbfs/mnt" in v):
        yield "DBFS_MNT_FUSE_USAGE_FOUND", "DBFS_MNT_FUSE_USAGE_FOUND"
    elif k != "" and isinstance(v, str) and ("dbfs:/" in v):
        yield "DBFS_ROOT_USAGE_FOUND", "DBFS_ROOT_USAGE_FOUND"
    elif k != "" and isinstance(v, str) and ("/dbfs/" in v):
        yield "DBFS_ROOT_FUSE_USAGE_FOUND", "DBFS_ROOT_FUSE_USAGE_FOUND"


def find_cluster_sources(k: str, v: Union[str, int, float, bool]) -> Iterator[Optional[Tuple[str, str]]]:
    # return issue_type and issue_detail
    if k.endswith("cluster_source"):
        yield "NOT_AN_ISSUE_CLUSTER_SOURCE", f"CLUSTER_SOURCE_IS_{v}"


def iter_flagged_issues(data: dict,
                        entity_id: str,
                        entity_name: str,
                        entity_type: Literal["WORKFLOW", "SUBMIT_RUN"],
                        entity_url: str,
                        workspace_url: str,
                        ) -> Optional[FlaggedIssue]:
    for k, v in generate_key_value_pairs(data):
        for issue_type, issue_detail in itertools.chain(
                find_dbfs_issue(k, v),
                find_runtime_issue(k, v),
                find_cluster_sources(k, v)
        ):
            yield FlaggedIssue(
                entity_type=entity_type,
                entity_id=entity_id,
                entity_name=entity_name,
                entity_issue_type=issue_type,
                entity_issue_detail=issue_detail,
                entity_issue_key=k,
                entity_issue_value=v,
                entity_url=entity_url,
                workspace_url=workspace_url,
                workspace_id=workspace_name
            )


def iter_cluster_ids(data: dict,
                     entity_id: str,
                     entity_name: str,
                     entity_type: Literal["WORKFLOW", "SUBMIT_RUN"],
                     entity_url: str,
                     workspace_url: str,
                     ) -> Optional[ClusterEntity]:
    for k, v in generate_key_value_pairs(data):
        k_last_part = k.split(".")[-1]
        if k_last_part in ["cluster_id", "existing_cluster_id"]:
            yield ClusterEntity(
                entity_type=entity_type,
                entity_id=entity_id,
                entity_name=entity_name,
                entity_cluster_id=v,
                entity_cluster_field=k,
                entity_url=entity_url,
                workspace_url=workspace_url,
                workspace_id=workspace_name
            )


def get_all_issues_from_job_runs(job_run: jobs.BaseRun, workspace_url: str, cluster_entities: dict) -> Iterator[
    FlaggedIssue]:
    # handle runtime issue
    data = job_run.as_dict()
    attach_cluster_entries(data, cluster_entities)
    yield from iter_flagged_issues(data,
                                   str(job_run.run_id),
                                   job_run.run_name,
                                   "SUBMIT_RUN",
                                   job_run.run_page_url,
                                   workspace_url)


def get_all_issues_from_job(job: jobs.Job, workspace_url: str, cluster_entities: dict) -> Iterator[FlaggedIssue]:
    # handle runtime issue
    data = job.as_dict()
    attach_cluster_entries(data, cluster_entities)
    yield from iter_flagged_issues(data,
                                   str(job.job_id),
                                   job.settings.name,
                                   "WORKFLOW",
                                   get_job_url(job, workspace_url),
                                   workspace_url)


def get_all_clusters_from_job_runs(job_run: jobs.BaseRun, workspace_url: str) -> Iterator[FlaggedIssue]:
    # handle runtime issue
    yield from iter_cluster_ids(job_run.as_dict(),
                                str(job_run.run_id),
                                job_run.run_name,
                                "SUBMIT_RUN",
                                job_run.run_page_url,
                                workspace_url)


def get_all_clusters_from_job(job: jobs.Job, workspace_url: str) -> Iterator[FlaggedIssue]:
    # handle runtime issue
    yield from iter_cluster_ids(job.as_dict(),
                                str(job.job_id),
                                job.settings.name,
                                "WORKFLOW",
                                get_job_url(job, workspace_url),
                                workspace_url)


def parse_json(json_str: str) -> Union[jobs.Job, jobs.BaseRun]:
    raw_data = json.loads(json_str)
    maybe_job_run = get_if_submit_run(raw_data)
    if maybe_job_run is None or maybe_job_run.run_id is None:
        return get_if_job(raw_data)
    # get_if_submit_run(raw_data)
    return maybe_job_run


def epoch_to_timestamp_str(timestamp_ms):
    timestamp_seconds = timestamp_ms / 1000
    dt_object = datetime.utcfromtimestamp(timestamp_seconds)

    # Convert the datetime object to a string format
    return dt_object.strftime('%Y-%m-%d %H:%M:%S')


def attach_cluster_entries(raw_data, cluster_entries):
    if cluster_entries is not None:
        raw_data["_internal_associated_clusters_mapping"] = {k: json.loads(v) for k, v in cluster_entries.items()}


def pdf_to_issues(data: pd.DataFrame) -> pd.DataFrame:
    resp = []
    for row in data.to_dict("records"):
        raw_json = row.get("_raw_data")
        workspace_url = row.get("_workspace_url")
        cluster_entities = row.get("_cluster_entries", None)
        job_or_run_obj = parse_json(raw_json)
        if isinstance(job_or_run_obj, jobs.Job):
            for iss in get_all_issues_from_job(job_or_run_obj, workspace_url, cluster_entities):
                if iss is not None:
                    iss.entity_create_ts_utc = epoch_to_timestamp_str(job_or_run_obj.created_time)
                    resp.append(asdict(iss))
        elif isinstance(job_or_run_obj, jobs.BaseRun):
            for iss in get_all_issues_from_job_runs(job_or_run_obj, workspace_url, cluster_entities):
                if iss is not None:
                    iss.entity_create_ts_utc = epoch_to_timestamp_str(job_or_run_obj.start_time)
                    resp.append(asdict(iss))
    return pd.DataFrame(resp)


def pdf_to_cluster_ids(data: pd.DataFrame) -> pd.DataFrame:
    resp = []
    for row in data.to_dict("records"):
        raw_json = row.get("_raw_data")
        workspace_url = row.get("_workspace_url")
        job_or_run_obj = parse_json(raw_json)
        if isinstance(job_or_run_obj, jobs.Job):
            for cluster in get_all_clusters_from_job(job_or_run_obj, workspace_url):
                if cluster is not None:
                    cluster.entity_create_ts_utc = epoch_to_timestamp_str(job_or_run_obj.created_time)
                    resp.append(asdict(cluster))
        elif isinstance(job_or_run_obj, jobs.BaseRun):
            for cluster in get_all_clusters_from_job_runs(job_or_run_obj, workspace_url):
                if cluster is not None:
                    cluster.entity_create_ts_utc = epoch_to_timestamp_str(job_or_run_obj.start_time)
                    resp.append(asdict(cluster))
    return pd.DataFrame(resp)


# COMMAND ----------

# DBTITLE 1,Retrieve all Used Clusters
cluster_usage_table_location = str(dbfs_folder_path / "analysis/clusters_used/delta")

spark.createDataFrame(pdf_to_cluster_ids(spark.sql(f"""
  SELECT _raw_data, _workspace_url FROM delta.`{workflows_table_location}` UNION ALL
  SELECT _raw_data, _workspace_url FROM delta.`{workflow_submit_runs_table_location}`
""").toPandas())).write.option("overwriteSchema", "true").mode("overwrite").save(cluster_usage_table_location)

print(cluster_usage_table_location)

# COMMAND ----------

cluster_ids = [row.entity_cluster_id for row in spark.sql(f"""
    SELECT distinct entity_cluster_id FROM delta.`{cluster_usage_table_location}`
""").collect() if row.entity_cluster_id not in ["", None]]
cluster_ids

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM 
# MAGIC delta.`dbfs:/tmp/uc_assessment/compute/analysis/clusters_used/delta` a
# MAGIC LEFT OUTER JOIN delta.`dbfs:/tmp/uc_assessment/compute/clusters/delta` b
# MAGIC ON a.entity_cluster_id = b.cluster_id

# COMMAND ----------

# DBTITLE 1,Ingest all clusters
cluster_details_table_location = str(dbfs_folder_path / "clusters/delta")
ClusterDetailsHandler(
    spark,
    cluster_details_table_location,
    client=client,
    cluster_ids=cluster_ids,
    workspace_name=workspace_name,
    workspace_url=workspace_url,
    buffer_size=200  # commit every 2000 workflows retrieved
).run()
print(cluster_details_table_location)

# COMMAND ----------

# DBTITLE 1,Ingest All Issues
query_df = spark.sql(f"""
WITH 
clusters_base AS (
  SELECT entity_id, entity_type, struct(entity_cluster_id, _raw_data) as _raw_cluster_json
  FROM
    ( SELECT distinct entity_id, entity_cluster_id, _raw_data, entity_type FROM 
        delta.`{cluster_usage_table_location}` a
        LEFT OUTER JOIN delta.`{cluster_details_table_location}` b
        ON a.entity_cluster_id = b.cluster_id
        WHERE _raw_data is not null
    )
),
submit_run_clusters as (
  SELECT entity_id, map_from_entries(cluster_jsons) as cluster_mapping
  FROM
  (SELECT entity_id, collect_list(_raw_cluster_json) as cluster_jsons
    FROM clusters_base WHERE entity_type = "SUBMIT_RUN" GROUP BY 1)
),
workflow_clusters AS (
  SELECT entity_id, map_from_entries(cluster_jsons) as cluster_mapping
  FROM
  (SELECT entity_id, collect_list(_raw_cluster_json) as cluster_jsons
   FROM clusters_base WHERE entity_type = "WORKFLOW" GROUP BY 1)
)
,
jobs_with_clusters AS (
  SELECT a.*, b.cluster_mapping as _cluster_entries
    FROM delta.`{workflows_table_location}` a
    LEFT OUTER JOIN workflow_clusters b
    ON a.job_id = b.entity_id
),
submit_runs_with_clusters AS (
  SELECT a.*, b.cluster_mapping as _cluster_entries
    FROM delta.`{workflow_submit_runs_table_location}` a
    LEFT OUTER JOIN submit_run_clusters b
    ON a.run_id = b.entity_id
)
SELECT _raw_data, _workspace_url, _cluster_entries FROM jobs_with_clusters
UNION ALL
SELECT _raw_data, _workspace_url, _cluster_entries FROM submit_runs_with_clusters
          """)

compute_issues_table_location = str(dbfs_folder_path / "analysis/compute_issues/delta")
(spark.createDataFrame(pdf_to_issues(query_df.toPandas()))
 .write
 .option("overwriteSchema", "true")
 .mode("overwrite")
 .save(compute_issues_table_location)
 )
print(compute_issues_table_location)

# COMMAND ----------

# %sql


# SELECT _workspace_url, entity_type, "TOTAL_ENTITIES" as metric_type, count(DISTINCT entity_id) as metric_value
# FROM
#   (SELECT "WORKFLOW" as entity_type, job_id as entity_id, _workspace_url FROM delta.`dbfs:/tmp/sri/wsjobs/delta` UNION ALL
#   SELECT "SUBMIT_RUN" as entity_type, run_id as entity_id, _workspace_url FROM delta.`dbfs:/tmp/sri/wsjobsruns/delta`)
#   GROUP BY 1, 2, 3
# UNION ALL
# SELECT workspace_url, entity_type, "TOTAL_ENTITIES_WITH_ISSUES" as metric_type, count(DISTINCT entity_id) as metric_value
# FROM delta.`dbfs:/tmp/uc_assessment/compute/analysis/compute_issues/delta`
# WHERE contains(entity_issue_type, "NOT_AN_ISSUE_") is false
# GROUP BY 1, 2, 3

# COMMAND ----------

# %sql

# SELECT workspace_url, entity_type, entity_issue_type, entity_issue_detail, sum(issue_ct) as issue_ct
# FROM
# (SELECT workspace_url, entity_type,entity_id, entity_issue_type, entity_issue_detail, count(1) as issue_ct FROM delta.`dbfs:/tmp/uc_assessment/compute/analysis/compute_issues/delta`
# GROUP BY 1, 2, 3, 4, 5)
# GROUP BY 1, 2, 3, 4
# ORDER BY 5 desc

# COMMAND ----------
