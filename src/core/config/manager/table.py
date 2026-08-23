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

    def get_table_deps(self, table_names: Union[str, List[str]], stage: StageType) -> Dict[str, Optional[List[TableDependency]]]:
        if isinstance(table_names, str):
            table_names = [table_names]

        dependencies: Dict[str, Optional[List[TableDependency]]] = {}

        for table_name in table_names:
            deps = self.get_table_config(table_name).depends_on

            if deps is None:
                dependencies[table_name] = []
                continue

            dependencies[table_name] = [
                TableDependency(
                    name=dep.name,
                    catalog=dep.catalog,
                    schema_name=dep.schema_name,
                )
                for dep in deps.get(stage, [])
            ]

        return dependencies

    def get_table_fullname(self, table_name: str, stage: StageType) -> str:
        catalog = self._catalog_manager.get_catalog_name()
        schema = self._schema_manager.get_stage_schema_name(stage)
        return create_table_fullname(catalog, schema, table_name)

    def get_table_metadata(self, table_ref: str, stage: StageType, query_params: Optional[Dict[str, str]] = None) -> TableMetadata:
        if stage is None:
            raise ValueError("Stage must be provided to get table metadata.")

        catalog = self._catalog_manager.get_catalog_name()
        schema_name = self._schema_manager.get_stage_schema_name(stage)
        upstream_stage = self._schema_manager.get_stage_upstream(stage)

        source_schema = self.get_table_schema(table_ref, upstream_stage)
        target_schema = self.get_table_schema(table_ref, stage)

        return TableMetadata(
            name=table_ref, 
            catalog=catalog,

            #namespace is the schema/namespace of the current stage, which is used for loading the table.
            namespace=schema_name,
            write_mode=self.get_table_write_mode(table_ref, stage),

            #For Extract, we need to get the full name of the table from the upstream stage, not the current stage.
            fullname=self.get_table_fullname(table_ref, upstream_stage), 

            #For load, we need to get the full name of the table from the current stage, not the upstream stage.
            location=self.get_table_fullname(table_ref, stage),

            #schema is structure of the table, which is used for validaton schema when extracting data from the upstream stage.
            source_schema=source_schema,
            target_schema=target_schema,
            queries=self.get_formated_query(table_ref, **(query_params or {})),
            query_params=query_params
        )
       
if __name__ == "__main__":
    table_manager = SchemaManager().get_stage_upstream("bronze")
    print(table_manager)
