from typing_extensions import Callable, Dict, Optional

from src.models.data_config import WriteType

from .base_load import BaseLoad

class IcebergLoad(BaseLoad):

    def load(self) -> None:
        try:
            writer = self.dataframe.writeTo(self.location)
            writer_action = self._resolve_write_mode(self.write_mode, writer)

            if self.write_mode == "custom":
                self.dataframe.createOrReplaceTempView(self.transform_result.query_params["table_view"])
                for query in self.queries:
                    writer_action(query)
            else:
                writer_action()

        except Exception as e:
            raise RuntimeError(f"Failed to load data for table '{self.transform_result.fullname}': {e}") from e

    def _resolve_write_mode(self, write_mode: WriteType, writer: Optional[object] = None) -> Callable:

        dispatch: Dict[str, Callable] = {
            "custom": self.session.sql,
            "append": writer.append,
            "overwrite": writer.replace,
            "overwrite_partitions": writer.overwritePartitions,
        }

        dispatch_action = dispatch.get(write_mode)

        return dispatch_action
