import solara

from assessment.code_scanner.utils import get_ws_browser_hostname, get_ws_client
from assessment.ui.models import WorkspaceConf

workspace_conf_ini: solara.Reactive[str] = solara.reactive("")
workspace_conf: solara.Reactive[WorkspaceConf] = solara.reactive(WorkspaceConf(configs={}))
repo_conf_toml: solara.Reactive[str] = solara.reactive("")
workspace_url = get_ws_browser_hostname() or get_ws_client(default_profile="uc-assessment-azure").config.host