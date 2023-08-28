from datetime import datetime
from pathlib import Path
from typing import List, Optional

import pandas as pd
from databricks.sdk.service.jobs import RunResultState, RunLifeCycleState
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Enum, UniqueConstraint, func, desc
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

Base = declarative_base()


# class JobRunState(PyEnum):
#     PENDING = "pending"
#     RUNNING = "running"
#     COMPLETED = "completed"
#     FAILED = "failed"

# class JobRunState(RunResultState):

class JobRun(Base):
    __tablename__ = "job_runs"

    id = Column(Integer, primary_key=True, index=True)
    run_id = Column(String, index=True)
    job_name = Column(String)
    run_url = Column(String, nullable=True)
    workspace_alias = Column(String, nullable=True)
    workspace_url = Column(String, index=True)
    lifecycle_state = Column(Enum(RunLifeCycleState), default=RunLifeCycleState.PENDING)
    result_state = Column(Enum(RunResultState), nullable=True)
    state_updated_time = Column(DateTime, default=datetime.utcnow)  # New field
    start_time = Column(DateTime, default=datetime.utcnow)
    end_time = Column(DateTime, nullable=True)

    __table_args__ = (UniqueConstraint('workspace_url', 'run_id', name='_workspace_run_uc'),)

    @staticmethod
    def to_dataframe(runs: List['JobRun']) -> pd.DataFrame:
        data = []
        for job_run in runs:
            data.append({
                'id': job_run.id,
                'run_id': job_run.run_id,
                'run_url': job_run.run_url,
                'workspace_url': job_run.workspace_url,
                'workspace_alias': job_run.workspace_alias,
                'lifecycle_state': job_run.lifecycle_state,
                'result_state': job_run.result_state,
                'state_updated_time': job_run.state_updated_time,
                'start_time': job_run.start_time,
                'end_time': job_run.end_time
            })

        df = pd.DataFrame(data)
        return df

    @staticmethod
    def to_display_dataframe(runs: List['JobRun']) -> pd.DataFrame:
        data = []
        for job_run in runs:
            if job_run.result_state is None:
                result_state = "Not Available"
            elif job_run.result_state == RunResultState.SUCCESS:
                result_state = f":white_check_mark: {job_run.result_state.value}"
            else:
                result_state = f":x: {job_run.result_state.value}"
            life_cycle_state = (job_run.lifecycle_state and job_run.lifecycle_state.value) or "PENDING"
            data.append({
                'id': job_run.id,
                'workspace_alias': job_run.workspace_alias,
                'run_id': job_run.run_id,
                'run_url': job_run.run_url,
                'workspace_url': job_run.workspace_url,
                'lifecycle_state': life_cycle_state,
                'result_state': result_state,
                'state_updated_time': job_run.state_updated_time,
                'start_time': job_run.start_time,
                'end_time': job_run.end_time
            })

        df = pd.DataFrame(data)
        return df


# Repository Abstraction
class JobRunRepository:
    def __init__(self, db_path: Path, create_if_not_exists=True, logging_enabled=False):
        db_path.parent.mkdir(parents=True, exist_ok=True)  # Ensure the parent directory exists
        self.engine = create_engine(f"sqlite:///{db_path}", echo=logging_enabled)
        if create_if_not_exists is True:
            JobRun.metadata.create_all(self.engine)
        self.SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=self.engine)

    def create_job_run(self, job_run: JobRun) -> JobRun:
        db = self.SessionLocal()
        try:
            db.add(job_run)
            db.commit()
            db.refresh(job_run)
        except IntegrityError as e:
            db.rollback()
            existing_job_run = (
                db.query(JobRun)
                .filter(JobRun.workspace_url == job_run.workspace_url, JobRun.run_id == job_run.run_id)
                .first()
            )
            return existing_job_run
        return job_run

    def update_job_run_state(self, run_state_updates: list):
        db = self.SessionLocal()
        updated_job_runs = []

        for run_id, lifecycle_state, result_state in run_state_updates:
            job_run = db.query(JobRun).filter(JobRun.run_id == run_id).first()
            if job_run:
                job_run.lifecycle_state = lifecycle_state
                if result_state:
                    job_run.result_state = result_state
                job_run.state_updated_time = datetime.utcnow()
                if lifecycle_state in [RunLifeCycleState.TERMINATED, RunLifeCycleState.SKIPPED]:
                    job_run.end_time = datetime.utcnow()
                updated_job_runs.append(job_run)

        db.commit()
        return updated_job_runs

    def get_latest_run_results(self, workspace_urls: Optional[List[str]] = None,
                               job_names: Optional[List[str]] = None,
                               num_runs: int = 5
                               ) -> List[JobRun]:
        db = self.SessionLocal()

        subquery = (
            db.query(
                JobRun,
                func.row_number().over(
                    partition_by=(JobRun.workspace_url, JobRun.job_name),
                    order_by=desc(JobRun.start_time)
                ).label("row_num")
            )
            .filter(JobRun.workspace_url.in_(workspace_urls) if workspace_urls else True)
            .filter(JobRun.job_name.in_(job_names) if job_names else True)
            .subquery()
        )

        query = (
            db.query(subquery)
            .filter(subquery.c.row_num <= num_runs)
            .order_by(subquery.c.workspace_url, subquery.c.job_name, subquery.c.start_time.desc())
        )

        latest_runs = query.all()

        return latest_runs

    #
    # def get_latest_run_results(self, workspace_urls: Optional[List[str]] = None,
    #                            job_names: Optional[List[str]] = None,
    #                            num_runs: int = 5
    #                            ) -> List[JobRun]:
    #     db = self.SessionLocal()
    #     query = db.query(JobRun).order_by(JobRun.workspace_url, JobRun.job_name, JobRun.start_time.desc())
    #
    #     if workspace_urls:
    #         query = query.filter(JobRun.workspace_url.in_(workspace_urls))
    #
    #     if job_names:
    #         query = query.filter(JobRun.job_name.in_(job_names))
    #
    #     query = query.group_by(JobRun.workspace_url, JobRun.job_name, JobRun.start_time).limit(num_runs)
    #     latest_runs = query.all()
    #
    #     return latest_runs

    def get_incomplete_run_ids(self, workspace_urls: list) -> List[str]:
        db = self.SessionLocal()
        incomplete_run_ids = (
            db.query(JobRun.run_id)
            .filter(
                JobRun.workspace_url.in_(workspace_urls),
                JobRun.lifecycle_state.notin_([RunLifeCycleState.TERMINATED,
                                               RunLifeCycleState.INTERNAL_ERROR,
                                               RunLifeCycleState.SKIPPED]),
            )
            .all()
        )
        return [run_id for (run_id,) in incomplete_run_ids]

    def get_latest_successful_run(self, workspace_url: str, job_name: str) -> Optional[JobRun]:
        db = self.SessionLocal()
        latest_successful_run = (
            db.query(JobRun)
            .filter(
                JobRun.workspace_url == workspace_url,
                JobRun.job_name == job_name,
                JobRun.result_state == RunResultState.SUCCESS
            )
            .order_by(JobRun.start_time.desc())
            .first()
        )
        return latest_successful_run

    def list(self):
        db = self.SessionLocal()
        return db.query(JobRun).all()

# if __name__ == "__main__":
#     job_run_repo = JobRunRepository(db_path=Path("/Users/sri.tikkireddy/PycharmProjects/uc-assessment-tools/tmp_db/test.db"))
#     # for i in range(10):
#     #     job_run_repo.create_job_run(JobRun(workspace_url="https://test.com", run_id=f"123_{i}"))
#     for i in job_run_repo.list():
#         print(i.id, i.run_id, i.workspace_url, i.lifecycle_state, i.state_updated_time, i.start_time, i.end_time)
#     # job_run_repo.update_job_run_state([("123", JobRunState.PENDING)])
#     runs = job_run_repo.get_latest_run_results(["https://test.com"])
#     df = JobRun.to_dataframe(runs)
#     print(df.head(10))
#         # print(i.id, i.run_id, i.workspace_url, i.state, i.state_updated_time, i.start_time, i.end_time)
#
#     for r_id in job_run_repo.get_incomplete_run_ids(["https://test.com"]):
#         print(r_id)
