from typing import cast, Tuple, List

import solara

from assessment.ui.models import WorkspaceConf
from assessment.ui.state import workspace_conf_ini


@solara.component
def ValidClientCheckList(selected_ws: solara.Reactive[List[str]],
                         clusters: bool = False,
                         warning_if_none_selected: bool = False):
    increment, set_increment = solara.use_state(0)
    error, set_error = solara.use_state("")
    aliases, set_aliases = solara.use_state(cast(List[Tuple[str, str]], None))

    def validate_ws():
        try:
            wc = WorkspaceConf.from_ini(workspace_conf_ini.value)
            valid_aliases: List[Tuple[str, str]] = []
            for alias, config in wc.configs.items():
                if config.ensure_cluster():
                    valid_aliases.append((alias, config.host))  # makes api call and checks if cluster exists
            set_aliases(valid_aliases)
        except Exception as e:
            set_error(str(e))
            return

    result = solara.use_thread(validate_ws, [increment])

    if result.state == solara.ResultState.FINISHED:
        if aliases is not None:
            if len(selected_ws.value) == 0 and warning_if_none_selected is True:
                solara.Warning("No workspaces selected; please configure in settings "
                               "and select workspaces in drop down.")
            else:
                solara.Info(f"Selected workspaces: {str(selected_ws.value)}")
            with solara.Column():
                choices: List[str] = [alias[0] for alias in aliases]
                solara.SelectMultiple(label="Workspaces", values=selected_ws, all_values=choices, dense=True)

    elif result.state == solara.ResultState.ERROR:
        solara.Error(f"Error occurred: {result.error}")
    else:
        solara.Info(f"Running... (status = {result.state})")
        solara.ProgressLinear(value=True)
