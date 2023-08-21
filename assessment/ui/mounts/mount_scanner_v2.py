# DESIGNED FOR MULTIPLE WORKSPACES
import base64
import json
from collections import defaultdict
from typing import List, cast, Optional, Callable

import pandas as pd
import solara
import solara.lab

from assessment.code_scanner.mounts import remote_mounts_pdf
from assessment.code_scanner.utils import zip_bytes
from assessment.ui.components.valid_client_checklist import ValidClientCheckList
from assessment.ui.models import WorkspaceConf
from assessment.ui.state import workspace_conf_ini


def get_raw_code(org_host_mapping: Optional[dict] = None, uc_mount_mapping: Optional[dict] = None):
    org_host_mapping = json.dumps(org_host_mapping or {}, indent=2)
    uc_mount_mapping = json.dumps(uc_mount_mapping or {}, indent=2)
    return f"""
uc_mnt_migration_org_host_mapping = {org_host_mapping}
uc_mnt_migration_uc_mount_mapping = {uc_mount_mapping}

    """ + """
def get_uc_mount_target(mnt_path: str, normalize: bool = True) -> str:
  workspace_id = spark.conf.get("spark.databricks.clusterUsageTags.orgId", None)
  workspace_url = spark.conf.get("spark.databricks.workspaceUrl", None) or uc_mnt_migration_org_host_mapping.get(workspace_id, None)
  mnt_path_mapping = uc_mnt_migration_uc_mount_mapping.get(workspace_id, {})
  mnt_target = mnt_path_mapping.get(mnt_path, None)
  if mnt_target is None:
    raise ValueError(f"Unable to find target storage location for {mnt_path}. The workspace id is: {workspace_id} and the host is: {workspace_url}. Here are the mountpath mappings available: {mnt_path_mapping}.")
  if normalize is True:
    mnt_target = mnt_target.rstrip("/")
  return mnt_target
"""


@solara.component
def CopyClipboardButton(content):
    content_b64 = base64.b64encode(content.encode("utf-8")).decode("utf-8")
    solara.HTML(tag="script", unsafe_innerHTML="""
        console.log("mounted clipboard button");
        var copyButtons = document.querySelectorAll(".copyToClipboardButton");

        copyButtons.forEach(function(button) {
            button.addEventListener("click", function() {
""" + f"""
                var textToCopy = atob("{content_b64}");
""" + """
                navigator.clipboard.writeText(textToCopy)
                    .then(function() {
                        console.log("Copied content to clipboard ...");
                    })
                    .catch(function(error) {
                        console.error("Failed to copy: ", error);
                    });
            });
        });
            """
                )
    solara.Button("Copy To Clipboard", classes=["copyToClipboardButton"],
                  style="margin-bottom: 10px; margin-top: 10px;")


@solara.component
def MountScannerV2(mounts: pd.DataFrame, set_mounts: Callable[[pd.DataFrame], None]):
    selected_ws: solara.Reactive[List[str]] = solara.use_reactive([])

    current_ws, set_current_ws = solara.use_state(cast(Optional[str], None))
    loading, set_loading = solara.use_state(False)
    progress_state, set_progress_state = solara.use_state("")
    error, set_error = solara.use_state("")

    def get_raw_data(csv=False, zip_file=True, file_name="mounts.zip"):
        df_copy = mounts.copy(deep=True)
        if csv is True:
            data = df_copy.to_csv(index=False)
        else:
            data = df_copy.to_parquet(index=False)

        if zip_file is True:
            return zip_bytes(data, file_name)

        return data

    def process_mounts(mounts_pdf: pd.DataFrame) -> str:
        org_host_mapping = {}
        uc_mount_mapping = defaultdict(dict)
        for record in mounts_pdf.to_dict(orient="records"):
            is_valid = record.get("is_mount_valid")
            if is_valid is True:
                ws_url = record.get("workspace_url")
                org_id = record.get("org_id")
                mnt_path = record.get("raw_src")
                mnt_target = record.get("target")
                org_host_mapping[org_id] = ws_url
                uc_mount_mapping[org_id][mnt_path] = mnt_target
        return get_raw_code(org_host_mapping, uc_mount_mapping)

    def get_mounts():
        set_loading(True)
        set_error("")
        wc = WorkspaceConf.from_ini(workspace_conf_ini.value)
        pdfs = []
        for alias in selected_ws.value:
            set_current_ws(alias)
            try:
                workspace_url = wc.configs[alias].host
                client = wc.configs[alias].get_ws_client()
                cluster_id = wc.configs[alias].get_cluster_id()
                mounts_pd = remote_mounts_pdf(client, cluster_id, "abfss", set_progress_state)
                mounts_pd['workspace_url'] = workspace_url
                pdfs.append(mounts_pd)
            except Exception as e:
                set_error(error + f"\nWorkspace: {alias} " + str(e))
                return
        set_mounts(pd.concat(pdfs, ignore_index=True))
        set_loading(False)

    with solara.Card("Download Mounts Info"):
        solara.Info("Note: This will ignore mounts: DatabricksRoot, DbfsReserved, UnityCatalogVolumes, "
                    "databricks/mlflow-tracking, databricks-datasets, databricks/mlflow-registry, databricks-results.")
        solara.Info("Note: This runs across all selected workspaces.")
        ValidClientCheckList(selected_ws, clusters=True)
        solara.Button("Load Mounts", style="margin-bottom: 25px", on_click=get_mounts,
                      disabled=loading or len(selected_ws.value) == 0)

        if loading is True:
            solara.Info(f"Loading... {current_ws}... status: {progress_state}")
            solara.ProgressLinear(True)
        elif mounts is not None:
            if error != "":
                solara.Error(error)
            solara.FileDownload(label="Download Mounts Info", filename="mounts.zip",
                                data=lambda: get_raw_data(csv=True, file_name="mounts.csv"))
            with solara.lab.Tabs():
                with solara.lab.Tab("Raw Data..."):
                    solara.DataFrame(mounts)
                with solara.lab.Tab("Shared Notebook Snippet"):
                    CopyClipboardButton(process_mounts(mounts))
                    solara.Markdown(f"```python{process_mounts(mounts)}```")
