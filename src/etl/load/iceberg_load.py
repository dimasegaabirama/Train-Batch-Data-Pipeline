from typing_extensions import Callable, Dict
from .base_load import BaseLoad


class IcebergLoad(BaseLoad):
    def __init__(self, stage, logger, session, config, table_name, write_mode, query = None, **extra):
        super().__init__(stage, logger, session, config, table_name, write_mode, query, **extra)
        self.full_table_name = self.resolve_full_table_name()

    def load(self) -> None:
        self.logger.debug(
            f"[IcebergLoad] Start writing to {self.full_table_name} with mode='{self.write_mode}'"
        )

        try:
            if self.write_mode == "custom":
                if not self.query:
                    raise ValueError("Mode 'custom' requires a SQL query.")
                self.session.sql(self.query)

            else:
                writer = self.dataframe.writeTo(self.full_table_name)

                dispatch: Dict[str, Callable] = {
                    "append": writer.append,
                    "overwrite": writer.replace,
                    "overwrite_partitions": writer.overwritePartitions,
                }

                action = dispatch.get(self.write_mode)
                if action is None:
                    raise ValueError(f"Unsupported write_mode: '{self.write_mode}'")

                action()

            self.logger.debug(f"[IcebergLoad] Successfully wrote to {self.full_table_name}")

        except Exception as e:
            self.logger.exception(
                f"[IcebergLoad] Failed writing to {self.full_table_name} with mode='{self.write_mode}': {e}"
            )
            raise
