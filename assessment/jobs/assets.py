import os
from pathlib import Path
from typing import List

from databricks.sdk import WorkspaceClient
from databricks.sdk.service.workspace import ImportFormat
from pydantic import BaseModel

from assessment.code_scanner.utils import log


class Upload(BaseModel):
    src_path: str
    file_name: str
    notebook_name: str
    content: str


def list_files_in_directory(directory_path):
    if not os.path.exists(directory_path):
        return "Directory not found."

    if not os.path.isdir(directory_path):
        return "The provided path is not a directory."

    files_list = os.listdir(directory_path)
    return files_list


def iter_uploads():
    import assessment.jobs.upload
    from pathlib import Path
    upload_src_dir = Path(assessment.jobs.upload.__file__).parent
    for file in os.listdir(upload_src_dir):
        if file.endswith(".py") and file != "__init__.py":
            with open(upload_src_dir / file, "rb") as f:
                yield Upload(
                    src_path=str(upload_src_dir / file),
                    file_name=file,
                    notebook_name=file.replace(".py", ""),
                    content=f.read().decode("utf-8")
                )


class AssetStatus(BaseModel):
    asset_name: str
    status: str


class AssetManager:

    def __init__(self, databricks_ws_client: WorkspaceClient):
        self._ws_client: WorkspaceClient = databricks_ws_client

    def list_uploaded_assets(self):
        return [obj_info.path.split("/")[-1] for obj_info in
                self._ws_client.workspace.list(str(self._get_standard_root_dir()))]

    @staticmethod
    def list_assets_to_be_uploaded():
        return [upload.file_name.rstrip(".py") for upload in iter_uploads()]

    def list_asset_status(self) -> List[AssetStatus]:
        uploaded_assets = self.list_uploaded_assets()
        return [AssetStatus(
            asset_name=upload_asset,
            status="UPLOADED" if upload_asset in uploaded_assets else "NOT_UPLOADED"
        ) for upload_asset in self.list_assets_to_be_uploaded()]

    def _current_user(self):
        return self._ws_client.current_user.me().user_name

    def _get_standard_root_dir(self) -> Path:
        return Path("/Users") / Path(self._current_user()) / Path(".uc_assessment/assets")

    def _ensure_assets_dir(self):
        parent_notebook_dir_path: Path = self._get_standard_root_dir()
        self._ws_client.workspace.mkdirs(str(parent_notebook_dir_path))  # ensures directory exists

    def _upload(self, upload: Upload):
        self._ensure_assets_dir()
        log.info("Uploading asset: %s to %s", upload.file_name, self.notebook_path(upload.file_name))
        self._ws_client.workspace.upload(self.notebook_path(upload.file_name), upload.content,
                                         format=ImportFormat.SOURCE, overwrite=True)

    def notebook_path(self, notebook_name: str):
        return str(self._get_standard_root_dir() / notebook_name)

    def upload_all(self):
        for upload in iter_uploads():
            self._upload(upload)

    def upload_just(self, notebook_name):
        for upload in iter_uploads():
            if upload.notebook_name == notebook_name:
                self._upload(upload)
                return upload

        raise Exception(f"Notebook {notebook_name} not found in upload list.")
