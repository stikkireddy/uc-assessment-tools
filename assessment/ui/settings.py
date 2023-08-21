from typing import cast, Optional

import pandas as pd
import solara
import solara.lab
from reacton import ipyvuetify as v

from assessment.code_scanner.utils import LOGS_FOLDER
from assessment.ui.components.log_browser import LogBrowser
from assessment.ui.models import WorkspaceConf
from assessment.ui.state import workspace_conf_ini, repo_conf_toml


@solara.component
def WorkspaceConfig():
    workspace_conf, set_workspace_conf = solara.use_state(cast(Optional[WorkspaceConf], None))
    workspace_conf_df, set_workspace_conf_df = solara.use_state(cast(Optional[pd.DataFrame], None))
    error, set_error = solara.use_state("")

    def redact_content(content: str) -> str:
        lines = content.split("\n")
        new_lines = []
        for line in lines:
            if line.strip().startswith("token"):
                new_lines.append("token = dapi**********")
            else:
                new_lines.append(line)
        return "\n".join(new_lines)

    solara.FileDrop(
        label="Upload Workspace Config",
        on_file=lambda file: workspace_conf_ini.set(file.get("data").decode("utf-8")),
        lazy=False
    )
    v.Textarea(
        v_model=redact_content(workspace_conf_ini.value),
        solo=True, hide_details=True, outlined=True, rows=1,
        disabled=True,
        auto_grow=True)

    def validate_ws():
        if workspace_conf_ini is None or workspace_conf_ini.value is None or workspace_conf_ini.value.strip() == "":
            return
        try:
            wc = WorkspaceConf.from_ini(workspace_conf_ini.value)
            set_workspace_conf_df(wc.to_validate_df())
            set_workspace_conf(wc)
        except Exception as e:
            set_error(str(e))
            return

    result = solara.use_thread(validate_ws, [workspace_conf_ini.value])

    if result.state == solara.ResultState.FINISHED:
        if workspace_conf_df is not None:
            solara.Info(f"Validated Results...")
            solara.DataFrame(df=workspace_conf_df)
    elif result.state == solara.ResultState.ERROR:
        solara.Error(f"Error occurred: {result.error}")
    else:
        solara.Info(f"Validating Workspace Configs...")
        solara.ProgressLinear(value=True)


@solara.component
def Settings():
    with solara.lab.Tabs():
        with solara.lab.Tab("Workspace Config"):
            with solara.Card("Configure Workspace Settings"):
                WorkspaceConfig()
        with solara.lab.Tab("Repos Config"):
            with solara.Card("Configure Repos Settings"):
                v.Textarea(
                    v_model=repo_conf_toml.value,
                    on_v_model=repo_conf_toml.set, solo=True, hide_details=True, outlined=True, rows=1,
                    auto_grow=True)
        with solara.lab.Tab("Manage Logs"):
            LogBrowser(LOGS_FOLDER)
