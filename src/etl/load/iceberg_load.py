from typing_extensions import Callable, Dict

from src.utils.table_utils import create_table_fullname

from .base_load import BaseLoad


class IcebergLoad(BaseLoad):
    def __init__(
        self, logger, session, config, stage, table_name, dataframe
    ):
        super().__init__(
            logger, session, config, stage, table_name, dataframe
        )

    def resolve_full_table_name(self) -> str:

        return create_table_fullname(
            catalog_name=self.config.get_catalog_name(),
            schema_name=self.config.get_schema_name(self.stage),
            table_name=self.table_name
        )

    def load(self) -> None:
        full_name = self.resolve_full_table_name()
        write_mode = self.get_write_mode(self.stage)
        query = self.get_query()

        self.logger.info(
            f"[IcebergLoad] Start writing to {full_name} with mode='{write_mode}'"
        )

        try:
            if write_mode == "custom":
                if not query:
                    raise ValueError("Mode 'custom' requires a SQL query.")
                self.session.sql(query)

            else:
                writer = self.dataframe.writeTo(full_name)

                dispatch: Dict[str, Callable] = {
                    "append": writer.append,
                    "overwrite": writer.replace,
                    "overwrite_partitions": writer.overwritePartitions,
                }

                action = dispatch.get(write_mode)
                if action is None:
                    raise ValueError(f"Unsupported write_mode: '{write_mode}'")

                action()

            self.logger.info(f"[IcebergLoad] Successfully wrote to {full_name}")

        except Exception as e:
            self.logger.error(
                f"[IcebergLoad] Failed writing to {full_name} with mode='{write_mode}': {e}"
            )
            raise
