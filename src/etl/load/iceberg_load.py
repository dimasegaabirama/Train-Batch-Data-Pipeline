from typing_extensions import Dict, List, Callable

from src.models.data_config import WriteType

from .base_load import BaseLoad

from pyspark.sql.readwriter import DataFrameWriterV2

class IcebergLoad(BaseLoad):
    """Loader that writes the transformed dataframe to an Iceberg table."""

    def _resolve_write_action(self, writer: DataFrameWriterV2, write_mode: WriteType) -> Callable:
        write_actions = {
            "custom": self._write_custom_queries,
            "replace": writer.createOrReplace,
            "append": writer.append,
            "overwrite": writer.overwrite,
            "overwrite_partitions": writer.overwritePartitions,
        }

        write_action = write_actions.get(write_mode)
        if write_action is None:
            raise ValueError(f"Unsupported write mode: '{write_mode}'")

        return write_action

    def _write_custom_queries(self, queries: List[str]) -> None:
        view_name = self.transform_result.view_name
        if view_name:
            self.session.sql(f"DROP VIEW IF EXISTS {view_name}")
            self.dataframe.createOrReplaceTempView(view_name)

        for query in queries:
            self.session.sql(query)

    def load(self) -> None:
        try:
            writer = self.dataframe.writeTo(self.target_fullname)
            write_action = self._resolve_write_action(writer, self.write_mode)

            if self.write_mode == "custom":
                write_action(self.queries)
            else:
                write_action()

        except Exception as e:
            raise RuntimeError(
                f"Failed to load data for table '{self.target_fullname}': {e}"
            ) from e
