from typing import cast, Optional

import pandas as pd
import solara
import solara.lab
from reacton import ipyvuetify as v

from assessment.ui.models import WorkspaceConf
from assessment.ui.state import workspace_conf_ini, repo_conf_toml


@solara.component
def ValidateWSConfigs():
    workspace_conf, set_workspace_conf = solara.use_state(cast(Optional[WorkspaceConf], None))
    workspace_conf_df, set_workspace_conf_df = solara.use_state(cast(Optional[pd.DataFrame], None))
    increment, set_increment = solara.use_state(0)
    error, set_error = solara.use_state("")

    def validate_ws():
        if increment == 0:
            return
        try:
            wc = WorkspaceConf.from_ini(workspace_conf_ini.value)
            set_workspace_conf_df(wc.to_validate_df())
            set_workspace_conf(wc)
        except Exception as e:
            set_error(str(e))
            return

    # result = None
    # if increment > 0:
    result = solara.use_thread(validate_ws, [increment])

    def on_click():
        set_workspace_conf(None)
        set_increment(increment + 1)

    with solara.Column():
        solara.Button("Validate Configs", color="primary", outlined=True, on_click=on_click)
        if result.state == solara.ResultState.FINISHED:
            if workspace_conf_df is not None:
                solara.DataFrame(df=workspace_conf_df)
        elif result.state == solara.ResultState.ERROR:
            solara.Error(f"Error occurred: {result.error}")
        else:
            solara.Info(f"Running... (status = {result.state})")
            solara.ProgressLinear(value=True)

@solara.component
def Settings():
    with solara.lab.Tabs():
        with solara.lab.Tab("Validate"):
            with solara.Card("Validate Configs"):
                ValidateWSConfigs()
        with solara.lab.Tab("Workspace Config"):
            with solara.Card("Configure Workspace Settings"):
                v.Textarea(
                    v_model=workspace_conf_ini.value,
                    on_v_model=workspace_conf_ini.set, solo=True, hide_details=True, outlined=True, rows=1,
                    auto_grow=True)
        with solara.lab.Tab("Repos Config"):
            with solara.Card("Configure Repos Settings"):
                v.Textarea(
                    v_model=repo_conf_toml.value,
                    on_v_model=repo_conf_toml.set, solo=True, hide_details=True, outlined=True, rows=1,
                    auto_grow=True)
