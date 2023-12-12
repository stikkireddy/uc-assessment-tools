# uc-assessment-tools

## Development Instructions

1. Make a local profile called:
   1. dbx config profile: `uc-assessment-tools`
2. run `pip install -e. `
3. run `make local`

## Instructions

### Pre-Requisites

1. Existing Databricks Workspace
2. Create Databricks Cluster 13.3 Beta ML LTS (non UC cluster)
3. Access from cluster to git repos with the code
4. Azure Devops Repo
   1. url: https://dev.azure.com/username/_git/<repo_name>
   2. user: <username>
   3. ado personal access token: [https token]
5. Github
   1. url https://github.com/[org]/repo.git
   2. user: [username]
   3. github personal access token: ghp_*********
      1. Make sure your github token is set to classic and has access to repos.

### Steps To Run the Repo Scanner

1. Go to the workspace and import this code as url
https://raw.githubusercontent.com/stikkireddy/uc-assessment-tools/main/notebooks/01_REPO_SCANNER.py
2. Connect to a cluster that is not UC enabled and is runtime 13.3 ML LTS
3. Run the notebook end to end
4. Click the link produced by the last cell
5. Create a cfg file with the following information (host, token and cluster_id for a cluster that exists in the
   workspace)

``` toml
[workspace1]
host  = <workspace-url>
token = <databricks token>
cluster_id = 0329-145545-rugby794

[workspace2]
host  = <workspace-url>
token = <databricks token>
cluster_id = 0807-225846-motto493
```

6. Go to settings and upload the cfg file
7. Go to home
8. Use the mount info, repo scanner and find and replace as needed; **find and replace** is purely experimental.

## Disclaimer

UC-Assessment-Tools is not developed, endorsed not supported by Databricks. It is provided as-is; no warranty is derived
from using this package. For more details, please refer to the license.