from datetime import timedelta
from typing import Optional, Union, Callable

import pandas as pd
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.compute import Language, ResultType, Results, CommandStatusResponse


def create_dataframe(results: Results) -> Optional[pd.DataFrame]:
    if results.result_type is not ResultType.TABLE:
        return None
    # Create an empty dictionary to store the converted data
    schema = results.schema
    data = results.data
    converted_data_list = []

    # Iterate through the schema and data lists simultaneously
    for row in data:
        converted_row = {}
        for schema_item, value in zip(schema, row):
            name = schema_item['name']
            data_type = schema_item['type'].strip('"')  # Removing double quotes

            # Convert the data to the specified data type
            if data_type == 'integer':
                converted_row[name] = int(value)
            elif data_type == 'float':
                converted_row[name] = float(value)
            else:
                converted_row[name] = value
        # Handle other data types as needed

        converted_data_list.append(converted_row)

        # Create a Pandas DataFrame from the list of dictionaries
    df = pd.DataFrame(converted_data_list)

    return df


class WorkspaceContextManager:
    def __init__(self, client: WorkspaceClient, cluster_id: str, set_state: Optional[Callable] = None):
        self.set_state = set_state
        self.client = client
        self.cluster_id = cluster_id
        self.contexts = {}

    def _set_state(self, state):
        if self.set_state is not None:
            self.set_state(state)

    def _create_context(self, lang: Language) -> None:
        self._set_state(f"Creating execution context for cluster {self.cluster_id} for {str(lang)}!")
        context_resp = self.client.command_execution.create_and_wait(
            cluster_id=self.cluster_id, language=lang, timeout=timedelta(minutes=20))
        self.contexts[lang] = context_resp.id

    def __enter__(self):

        self._set_state(f"Ensuring cluster {self.cluster_id} is running!")
        self.client.clusters.ensure_cluster_is_running(cluster_id=self.cluster_id)
        # for lang in [Language.PYTHON, Language.SCALA]:
        #     self._create_context(lang)

        return self

    def __exit__(self, exc_type, exc_value, traceback):
        for lang, context_id in self.contexts.items():
            self.client.command_execution.destroy(cluster_id=self.cluster_id, context_id=context_id)
            self._set_state(f"Destroying execution context for cluster {self.cluster_id} for {str(lang)}!")

    def execute_python(self, command, as_pdf: bool = True) -> Union[Results, Optional[pd.DataFrame]]:
        return self._execute(Language.PYTHON, command, as_pdf)

    def execute_scala(self, command, as_pdf: bool = True) -> Union[Results, Optional[pd.DataFrame]]:
        return self._execute(Language.SCALA, command, as_pdf)

    def get_spark_config(self, key: str) -> Optional[str]:
        val = self._execute(Language.PYTHON, f"print(spark.conf.get('{key}', 'unknown'))", as_pdf=False).data
        if val == "unknown":
            return None
        return val

    def _execute(self, lang, command, as_pdf: bool = True) -> Union[Results, Optional[pd.DataFrame]]:
        context_id = self.contexts.get(lang)
        if context_id is None:
            self._create_context(lang)
            context_id = self.contexts[lang]

        self._set_state(f"Running command {str(command[:10])}...")
        resp: CommandStatusResponse = self.client.command_execution.execute_and_wait(
            cluster_id=self.cluster_id,
            context_id=context_id,
            command=command,
            language=lang,
            timeout=timedelta(minutes=20),
        )

        if as_pdf is True and resp.results.result_type is ResultType.TABLE:
            return create_dataframe(resp.results)

        return resp.results
