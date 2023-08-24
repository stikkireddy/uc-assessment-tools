from typing import Callable

import pandas as pd
import solara

from assessment.code_scanner.mounts import mounts_pdf
from assessment.code_scanner.utils import zip_bytes
from assessment.ui.state import workspace_url

# DESIGNED FOR SINGLE WORKSPACE
@solara.component
def MountScanner(mounts: pd.DataFrame, set_mounts: Callable[[pd.DataFrame], None]):
    loading, set_loading = solara.use_state(False)

    def get_raw_data(csv=False, zip_file=True, file_name="mounts.zip"):
        df_copy = mounts.copy(deep=True)
        df_copy['workspace_url'] = workspace_url
        if csv is True:
            data = df_copy.to_csv(index=False)
        else:
            data = df_copy.to_parquet(index=False)

        if zip_file is True:
            return zip_bytes(data, file_name)

        return data

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
            solara.FileDownload(label="Download Mounts Info", filename="mounts.zip",
                                data=lambda: get_raw_data(csv=True, file_name="mounts.csv"))
            solara.DataFrame(mounts)
