import os
from pathlib import Path
from typing import List, cast, Optional

import solara

from assessment.code_scanner.utils import zip_bytes


@solara.component
def LogBrowser(exec_base_path, exclude_prefixes: List[str] = None):
    file, set_file = solara.use_state(cast(Optional[Path], None))
    path, set_path = solara.use_state(cast(Optional[Path], None))
    file_content, set_file_content = solara.use_state(cast(Optional[str], None))
    EXECUTION_BASE_PATH = Path(exec_base_path).resolve()
    directory = solara.use_reactive(EXECUTION_BASE_PATH)
    message = solara.use_reactive(None)
    MAX_FILE_CT = 10000
    exclude_prefixes = exclude_prefixes or []

    with solara.Column():
        def filter_path(p: Path) -> bool:
            if any([str(p).startswith(prefix) for prefix in exclude_prefixes]):
                return False
            return True

        def protect():
            def check_base_path(value):
                if not str(value).startswith(str(EXECUTION_BASE_PATH)):
                    directory.value = EXECUTION_BASE_PATH
                    message.value = f"Cannot leave root base path {EXECUTION_BASE_PATH}!"
                else:
                    message.value = None

            return directory.subscribe(check_base_path)

        solara.use_effect(protect)
        if message.value:
            error = message.value
        elif path is None:
            error = "You must select a project root!"
        else:
            error = None

        def count_dir():
            count = 0
            for root, dirs, files in os.walk(str(path), topdown=False):
                for _ in files:
                    count += 1
                    if count > MAX_FILE_CT:
                        return count
            return count

        def download_dir():
            if path is not None and path.is_dir():
                import io
                import zipfile
                zip_buffer = io.BytesIO()
                zf = zipfile.ZipFile(zip_buffer, mode='w')

                def remove_prefix(text, prefix):
                    if text.startswith(prefix):
                        return text[len(prefix):]
                    return text

                for root, dirs, files in os.walk(str(path), topdown=False):
                    for name in files:
                        # zf.writestr()
                        this_file = str(os.path.join(root, name))
                        in_zip_name = remove_prefix(this_file, str(path) + "/")
                        with open(this_file, "rb") as f:
                            zf.writestr(in_zip_name, f.read())
                zf.close()
                return zip_buffer

        def last_10000_lines(p):
            with open(p, "r") as f:
                lines = f.readlines()
                return "".join(lines[-10000:])

        def on_path_select(p: Path) -> None:
            if str(p).startswith(str(EXECUTION_BASE_PATH)):
                set_path(p)
                try:
                    set_file_content(last_10000_lines(p))
                except Exception as e:
                    print(f"Error reading file: {e}")
                message.value = None

        def empty_file(file_path):
            try:
                with open(file_path, 'w') as f:
                    f.truncate(0)
                set_file_content("")
                print(f"Contents of '{file_path}' have been emptied.")
            except Exception as e:
                print(f"Error emptying file '{file_path}': {e}")

        if path is not None and path.is_file():
            solara.Info(f"You selected file for download: {path}")
            # must be lambda otherwise will always try to download
            with solara.HBox():
                solara.FileDownload(lambda: zip_bytes(path.open("rb").read(), path.name), path.name + ".zip",
                                    label=f"Download {path.name}.zip")
                solara.Button(f"Clear Logs {path.name}", on_click=lambda: empty_file(str(path)),
                              style="margin-left: 25px")

        if path is not None and path.is_dir():
            file_ct = count_dir()
            if file_ct >= MAX_FILE_CT:
                solara.Error(f"Too many files in directory unable to offer download ({file_ct} > {MAX_FILE_CT})")
            else:
                solara.Info(f"You selected directory for download as zip: {path}")
                # solara.Button("Download Directory From DBFS", on_click=download_dir)
                zip_name = path.name + ".zip"
                solara.FileDownload(lambda: download_dir(), zip_name, label=f"Download {file_ct} files in "
                                                                            f"{zip_name}")

        with solara.lab.Tabs():
            with solara.lab.Tab("Log File Browser"):
                solara.FileBrowser(
                    directory,
                    filter=filter_path,
                    on_path_select=on_path_select,
                    on_file_open=set_file,
                    can_select=True,
                ).key("file-browser")
            if path is not None:
                with solara.lab.Tab("Log Viewer"):
                    with solara.VBox():
                        with solara.Card(f"Last 10k Logs: {path}"):
                            solara.Button(f"Refresh", on_click=lambda: set_file_content(last_10000_lines(path)),
                                          style="margin-left: 25px; margin-bottom: 25px")
                            solara.Markdown("```" + (file_content or "\n") + "```", style="max-width: 100%; "
                                                                                          "max-height: 500px; "
                                                                                          "overflow: scroll;")
