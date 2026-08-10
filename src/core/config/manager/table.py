from typing import Optional

from typing_extensions import Dict, List, Union

from src.core.config.config import Config
from src.models.data_config import (
    StageType,
    TableContext,
    TableDependency,
    TableMetadata,
    TablesConfig,
)
from src.utils.table_utils import create_table_fullname

from .catalog import CatalogManager
from .schema import SchemaManager


class TableManager:
    def __init__(self):
        self._config = Config.get_config()
        self._catalog_manager = CatalogManager()
        self._schema_manager = SchemaManager()

    def get_config(self) -> TablesConfig:
        return self._config.tables

    def get_tablenames(self, stage: StageType) -> List[str]:
        cfg = self._config.pipeline.tablenames.get(stage)
        if cfg is None:
            raise ValueError(
                f"Tablenames for stage '{stage}' not found"
            )
        return cfg

    def get_table_config(self, table_name: str) -> TableContext:
        return getattr(self.get_config(), table_name)

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

    def get_formated_query(self, table_name: str, **kwargs):
        return [query.format(**kwargs) for query in self.get_table_query(table_name)]

    def get_table_schema(self, table_name: str, stage: StageType) -> str:
        cfg = self.get_table_config(table_name).schema.get(stage)
        if cfg is None:
            raise ValueError(
                f"Schema for table '{table_name}' stage '{stage}' not found"
            )
        return cfg

    def get_table_deps(self, table_names: Union[str, List[str]], stage: StageType) -> Dict[str, List[TableDependency]]:
        if isinstance(table_names, str):
            table_names = [table_names]

        dependencies = {}

        for table_name in table_names:
            if table_name not in dependencies:
                dependencies[table_name] = []

            deps = self.get_table_config(table_name).depends_on.get(stage, [])
            for dep in deps:
                dependencies[table_name].append({
                    "name": dep.name,
                    "catalog": dep.catalog,
                    "schema_name": dep.schema_name,
                })

        return dependencies

    def get_table_metadata(self, table_ref: str, stage: StageType, query_params: Dict[str, str] = None) -> TableMetadata:
        if stage is None:
            raise ValueError("Stage must be provided to get table metadata.")

        catalog = self._catalog_manager.get_catalog_name()
        schema = self._schema_manager.get_stage_schema_name(stage)
        upstream_stage = self._schema_manager.get_stage_upstream(stage)

        return {
            "name": table_ref,
            "catalog": catalog,
            "schema_name": schema,
            "write_mode": self.get_table_write_mode(table_ref, stage),
            "fullname": create_table_fullname(catalog, upstream_stage, table_ref),
            "location": create_table_fullname(catalog, stage, table_ref),  # Assuming location is the same as fullname for this case
            "schema": self.get_table_schema(table_ref, upstream_stage),
            "queries": self.get_formated_query(table_ref, **query_params)
        }

if __name__ == "__main__":
    table_manager = TableManager().get_table_deps("cancellation_summary", "gold")
    print(table_manager)
