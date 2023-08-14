# uc-assessment-tools

## Instructions

### Pre-Requisites

1. Databricks Workspace
2. Databricks Cluster 13.3 Beta ML LTS
3. Azure Devops Repo
   1. url: https://dev.azure.com/<username>/_git/<repo_name>
   2. user: <username>
   3. ado personal access token: <https token>
4. Github
   1. url https://github.com/<org>/<repo>.git
   2. user: <username>
   3. github personal access token: ghp_*********
      1. Make sure your github token is set to classic and has access to repos.

### Steps To Run the Repo Scanner

1. Go to the workspace and import this code as url
https://raw.githubusercontent.com/stikkireddy/uc-assessment-tools/main/notebooks/01_REPO_SCANNER.py
2. Connect to a cluster that is not UC enabled and is runtime 13.3 ML LTS
3. Run the notebook end to end
4. Run the last cell as many times as needed to render the widgets (the first time may not work)
    1. This is just a databricks quirk
5. In the widget/ui go to Mount Info tab, run it and then download the mount info csv file
6. In the widget/ui go to Repo Scanner tab, enter your repo info, click scan and download issues
