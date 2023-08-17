from typing import cast, Optional, Tuple, List

import pandas as pd
import solara

from assessment.ui.models import WorkspaceConf
from assessment.ui.state import workspace_conf_ini


@solara.component
def ValidClientCheckList(selected_ws: solara.Reactive[List[str]], clusters: bool = False):
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


    # result = None
    # if increment > 0:
    result = solara.use_thread(validate_ws, [increment])

    if result.state == solara.ResultState.FINISHED:
        if aliases is not None:
            solara.Info(f"Selected workspaces: {str(selected_ws.value)}")
            with solara.Column():
                for alias in aliases:
                    def modify_list_with_bool(bool_value, name=alias[0]):
                        if bool_value is True:
                            if name not in selected_ws.value:
                                selected_ws.value = selected_ws.value + [name]
                        else:
                            if name in selected_ws.value:
                                selected_ws.value = [name for name in selected_ws.value if name != name]

                    solara.Checkbox(label=f"{alias[0]}: {alias[1]}", value=False,
                                    on_value=modify_list_with_bool)
    elif result.state == solara.ResultState.ERROR:
        solara.Error(f"Error occurred: {result.error}")
    else:
        solara.Info(f"Running... (status = {result.state})")
        solara.ProgressLinear(value=True)
