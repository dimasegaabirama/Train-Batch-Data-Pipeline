from typing_extensions import Callable, Dict

from src.models.data_config import WriteType

from .base_load import BaseLoad


class IcebergLoad(BaseLoad):
    """Loader that writes the transformed dataframe to an Iceberg table."""

    def load(self) -> None:
        try:
            writer = self.dataframe.writeTo(self.location)
            writer_action = self._resolve_write_mode(self.write_mode, writer)

            if self.write_mode == "custom":
                view_name = self.transform_result.query_params["table_view"]
                self.dataframe.createOrReplaceTempView(view_name)
                for query in self.queries:
                    writer_action(query)
            else:
                writer_action()

        except Exception as e:
            raise RuntimeError(
                f"Failed to load data for table '{self.location}': {e}"
            ) from e

    def _resolve_write_mode(self, write_mode: WriteType, writer: object) -> Callable:
        """Map a write mode to the writer action that performs it."""
        dispatch: Dict[str, Callable] = {
            "custom": self.session.sql,
            "append": writer.append,
            "overwrite": writer.replace,
            "overwrite_partitions": writer.overwritePartitions,
        }

        return dispatch.get(write_mode)