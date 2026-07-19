from typing_extensions import Callable, Dict
from .base_load import BaseLoad
from src.utils.text_utils import clean_multiple_line

class IcebergLoad(BaseLoad):

    def load(self) -> None:
        self.logger.debug(
            f"[IcebergLoad] Start writing to {self.table_fullname} with mode='{self.write_mode}'"
        )

        try:
            if self.write_mode == "custom":
                if not self.queries:
                    raise ValueError("Mode 'custom' requires a SQL query.")
  
                for query in self.queries:
                    self.logger.debug(f"Using Query : {clean_multiple_line(query)}")
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

            self.logger.debug(f"[IcebergLoad] Successfully wrote to {self.table_fullname}")

        except Exception as e:
            self.logger.exception(
                f"[IcebergLoad] Failed writing to {self.table_fullname} with mode='{self.write_mode}': {e}"
            )
            raise
