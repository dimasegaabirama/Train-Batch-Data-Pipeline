from typing_extensions import Callable, Dict

from .base_load import BaseLoad
from src.utils.table_utils import create_table_fullname

class IcebergLoad(BaseLoad):

    def load(self) -> None:

        location
        write_mode = self.transform_result.write_mode
        queries = self.transform_result.queries

        try:
            if write_mode == "custom":
                if not queries:
                    raise ValueError("Mode 'custom' requires a SQL query.")
  
                for query in queries:
                    self.session.sql(query)

            else:
                writer = self.dataframe.writeTo(self.table_fullname)

                dispatch: Dict[str, Callable] = {
                    "append": writer.append,
                    "overwrite": writer.replace,
                    "overwrite_partitions": writer.overwritePartitions,
                }

                action = dispatch.get(self.write_mode)
                if action is None:
                    raise ValueError(f"Unsupported write_mode: '{self.write_mode}'")

                action()

        except Exception as e:
            raise RuntimeError(f"Failed to load data for table '{self.table_fullname}': {e}") from e
