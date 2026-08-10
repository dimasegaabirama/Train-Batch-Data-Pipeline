from typing_extensions import Callable, Dict, Optional

from src.models.data_config import WriteType

from .base_load import BaseLoad
from src.utils.table_utils import create_table_fullname

class IcebergLoad(BaseLoad):

    def load(self) -> None:

        location = self.transform_result.location
        write_mode = self.transform_result.write_mode
        queries = self.transform_result.queries

        try:
            writer = self.dataframe.writeTo(location)
            writer_action = self._resolve_write_mode(write_mode, writer)

            if self.write_mode == "custom":
                for query in queries:
                    writer_action(query)
            else:
                writer_action()

        except Exception as e:
            raise RuntimeError(f"Failed to load data for table '{self.table_fullname}': {e}") from e

    def _resolve_write_mode(self, write_mode: WriteType, writer: Optional[object] = None) -> Callable:

        dispatch: Dict[str, Callable] = {
            "custom": self.session.sql,
            "append": writer.append,
            "overwrite": writer.replace,
            "overwrite_partitions": writer.overwritePartitions,
        }

        dispatch_action = dispatch.get(write_mode)

        return dispatch_action
