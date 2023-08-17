import configparser
from typing import Optional, Dict

from databricks.sdk import WorkspaceClient
from pydantic import BaseModel

from assessment.code_scanner.multi_ws import WorkspaceContextManager


class DatabricksConfig(BaseModel):
    host: str
    token: Optional[str] = None
    cluster_id: Optional[str] = None
    client_id: Optional[str] = None
    client_secret: Optional[str] = None

    def get_ws_client(self) -> WorkspaceClient:
        if self.token is not None:
            return WorkspaceClient(host=self.host,
                                   token=self.token)
        elif self.client_id is not None and self.client_secret is not None:
            return WorkspaceClient(host=self.host,
                                   client_id=self.client_id,
                                   client_secret=self.client_secret)
        raise ValueError("Invalid config")

    def can_make_execution_context(self):
        return self.cluster_id is not None

    def execution_context(self):
        client = self.get_ws_client()
        return WorkspaceContextManager(client, self.cluster_id)

    def get_cluster_id(self) -> Optional[str]:
        return self.cluster_id

    def check_cluster(self):
        if self.cluster_id is not None:
            self.get_ws_client().clusters.get(self.cluster_id)

    def ensure_cluster(self) -> bool:
        if self.cluster_id is not None:
            try:
                self.check_cluster()
                return True
            except Exception as e:
                return False

        return False

    def ensure_access(self):
        try:
            self.check_access()
            return True
        except Exception as e:
            return False

    def check_access(self):
        self.get_ws_client().current_user.me()


def hide_secret(d: dict, key: str) -> dict:
    if key in d and d[key] is not None:
        d[key] = d[key][:5] + "********"
    return d


class WorkspaceConf(BaseModel):
    configs: Dict[str, DatabricksConfig]

    @classmethod
    def from_ini(cls, ini_str: str) -> "WorkspaceConf":
        config = configparser.ConfigParser()
        config.read_string(ini_str)
        databricks_configs = {}
        for section_name, config_data in config.items():
            # Skip default section if it is empty
            if section_name == "DEFAULT" and len(dict(config_data).keys()) == 0:
                continue
            databricks_configs[section_name] = DatabricksConfig(**dict(config_data))
        return cls(configs=databricks_configs)

    @staticmethod
    def _validate_access(client: WorkspaceClient):
        client.current_user.me()

    @staticmethod
    def _validate_cluster(client: WorkspaceClient, cluster_id: str):
        client.clusters.get(cluster_id)

    def to_validate_df(self):
        import pandas as pd
        res = []
        for section_name, config_data in self.configs.items():
            d = config_data.model_dump()
            d["alias"] = section_name
            hide_secret(d, "token")
            hide_secret(d, "client_secret")
            valid = True
            auth_error = None
            cluster_error = None
            try:
                config_data.check_access()
            except Exception as e:
                valid = False
                auth_error = str(e)
            try:
                config_data.check_cluster()
            except Exception as e:
                valid = False
                cluster_error = str(e)
            d["valid"] = valid
            d["auth_error"] = auth_error
            d["cluster_error"] = cluster_error
            res.append(d)
        return pd.DataFrame.from_records(res)
