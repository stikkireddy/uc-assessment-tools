from databricks.sdk import WorkspaceClient

from assessment.code_scanner.scan import in_dbx_notebook, get_dbutils


def get_ws_client(default_profile="uc-assessment-azure"):
    if in_dbx_notebook():
        _dbutils = get_dbutils()
        host = _dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiUrl().getOrElse(None),
        token = _dbutils.notebook.entry_point.getDbutils().notebook().getContext().apiToken().getOrElse(None)
        return WorkspaceClient(host=host, token=token)
    return WorkspaceClient(profile=default_profile)