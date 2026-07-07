from typing_extensions import Dict, List, Union

from src.core.config import Config
from src.models.data_config import (
    StageType,
    TableContext,
    TablesConfig,
)


class TableManager:
    def __init__(self):
        self._config = Config()

    def get_config(self) -> TablesConfig:
        return self._config.tables

    def get_tablenames(self) -> List[str]:
        return self._config.pipeline.tablenames

    def get_table_config(self, table_name: str) -> TableContext:
        return getattr(self.get_all_config(), table_name)

    def get_table_type(self, table_name: str) -> str:
        return self.get_table_config(table_name).type

    def get_table_partitioned_by(self, table_name: str) -> str:
        return self.get_table_config(table_name).partitioned_by

    def get_table_write_mode(self, table_name: str, stage: StageType) -> str:
        cfg = self.get_table_config(table_name).write_mode.get(stage)
        if cfg is None:
            raise ValueError(
                f"Write mode for table '{table_name}' stage '{stage}' not found"
            )
        return cfg

    def get_table_query(self, table_name: str) -> List[str]:
        return self.get_table_config(table_name).query

    def get_table_schema(self, table_name: str, stage: StageType) -> str:
        cfg = self.get_table_config(table_name).schema.get(stage)
        if cfg is None:
            raise ValueError(
                f"Schema for table '{table_name}' stage '{stage}' not found"
            )
        return cfg

    def get_table_deps(self, table_names: Union[str, List[str]]) -> Dict[str, str]:
        if isinstance(table_names, str):
            table_names = [table_names]

        dependencies: Dict[str, str] = {}

        for table_name in table_names:
            deps = self.get_table_config(table_name).depends_on or {}
            dependencies.update(
                {
                    name: f"{ctx['catalog']}.{ctx['schema']}.{name}"
                    for name, ctx in deps.items()
                }
            )

        return dependencies
