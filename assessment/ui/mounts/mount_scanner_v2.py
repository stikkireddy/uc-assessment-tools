# DESIGNED FOR MULTIPLE WORKSPACES
from typing import List

import solara

from assessment.ui.components.valid_client_checklist import ValidClientCheckList


@solara.component
def MountScannerV2():
    with solara.Card("Download Mounts Info"):
        solara.Info("Note: This will ignore mounts: DatabricksRoot, DbfsReserved, UnityCatalogVolumes, "
                    "databricks/mlflow-tracking, databricks-datasets, databricks/mlflow-registry, databricks-results.")
        solara.Info("Note: This runs across all selected workspaces.")

        selected_ws: solara.Reactive[List[str]] = solara.use_reactive([])
        ValidClientCheckList(selected_ws, clusters=True)

        solara.Button("Load Mounts", style="margin-bottom: 25px")
