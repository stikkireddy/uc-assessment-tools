from typing import Callable

import pandas as pd
import solara


@solara.component
def CodeFindAndReplace(issues: pd.DataFrame = None, _: Callable[[pd.DataFrame], None] = None):

    branch, set_branch = solara.use_state("uc_convert_")
    repo_url, set_repo_url = solara.use_state("")
    user, set_user = solara.use_state("")
    token, set_token = solara.use_state("")

    with solara.Card("Find And Replace"):
        if issues is None:
            solara.Error("No issues found!")

        solara.Markdown(f"### Find and Replace in branch: {branch} for repo: {repo_url}")
        solara.InputText("Branch", value=branch, on_value=set_branch,
                         error="Branch name must start with 'uc_convert_'"
                         if not branch.startswith("uc_convert_") else None, continuous_update=True)
        solara.InputText("Repo Url", value=repo_url, on_value=set_repo_url, continuous_update=True)
        solara.InputText("User Name", value=user, on_value=set_user, continuous_update=True)
        solara.InputText("Token", value=token, on_value=set_token, password=True, continuous_update=True)
        solara.Button("Find and Replace", style="margin-bottom: 25px",
                      # on_click=get_issues,
                      disabled=issues is None)

