import os
import shutil
import subprocess
from contextlib import contextmanager

import git

from assessment.code_scanner.utils import log


def set_git_user_for_commit(email, full_name):
    # Set user email and name configurations
    email_command = ['git', 'config', '--global', 'user.email', email]
    name_command = ['git', 'config', '--global', 'user.name', full_name]

    # Execute the commands
    subprocess.run(email_command, check=True)
    subprocess.run(name_command, check=True)


@contextmanager
def git_repo(repo_url,
             branch,
             path,
             email,
             full_name,
             delete=False,
             username=None,
             password=None):
    if username is not None and username != "" and password is not None and password != "":
        built_repo_url = repo_url.replace("https://", f"https://{username}:{password}@")
    else:
        built_repo_url = repo_url

    log.info(f"Cloning repo {repo_url} to {path}; branch: {branch}; delete: {delete}; username: {username};")

    # Clean up the path if it exists
    if path and os.path.exists(path):
        shutil.rmtree(path)

    # Clone the repo's main branch
    repo = git.Repo.clone_from(built_repo_url, path)

    log.info(f"Repository cloned to {path}")

    try:
        if delete and branch:
            # Delete the remote branch if it exists
            remote = repo.remote()
            try:
                remote.push(refspec=f":{branch}")
                log.info(f"Remote branch {branch} deleted.")
            except git.exc.GitCommandError:
                log.info(f"Remote branch {branch} doesn't exist.")

        # If branch is provided, create and switch to the new branch
        if branch:
            log.info(f"Branch: {branch} provided. Checking out.")
            remote_branch_exists = False
            remote = repo.remote()
            for ref in remote.refs:
                if ref.remote_head == branch:
                    remote_branch_exists = True
                    break

            if remote_branch_exists:
                log.info("Branch already exists. Checking out.")
                repo.git.checkout(branch)
            else:
                log.info("Branch doesn't exist. Creating and checking out.")
                new_branch = repo.create_head(branch)
                repo.head.reference = new_branch
        else:
            log.info("No branch provided. Using the main branch.")

        yield repo

    finally:
        # Check if there are any changes to commit
        if branch and repo.is_dirty():
            set_git_user_for_commit(email, full_name)
            repo.git.add(all=True)
            repo.git.commit('-m', 'Changes from context manager')
            # Set up the upstream tracking reference
            repo.remotes["origin"].push(branch)
            repo.git.branch("--set-upstream-to", f"origin/{branch}", branch)
            repo.git.push()
            log.info("Changes committed and pushed.")
        else:
            log.info(f"No changes to commit for branch: {branch}.")

        repo.close()
