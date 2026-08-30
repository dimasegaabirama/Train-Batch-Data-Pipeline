from typing import Optional

from typing_extensions import Dict, List, Union

from src.core.config.config import Config
from src.models.data_config import (
    BronzeSilverTableMetadata,
    GoldTableMetadata,
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

    def get_table_schema(self, table_name: str, stage: StageType) -> Optional[str]:
        cfg = self.get_table_config(table_name).table_schema.get(stage)
        return cfg

    def get_table_deps(self, table_name: str, stage: StageType) -> Optional[Dict[str, List[TableDependency]]]:

        dependencies: Dict[str, Optional[List[TableDependency]]] = {}

        raw = getattr(self.get_table_config(table_name), 'depends_on', None) or {stage: None}
        deps = raw.get(stage)

        if deps is None:
            return None
        else:
            dependencies[table_name] = [
                TableDependency(
                    name=dep.name,
                    catalog=dep.catalog,
                    namespace=dep.namespace,
                )
                for dep in deps
            ]
        return dependencies


    def get_table_fullname(self, table_name: str, stage: StageType) -> str:
        catalog = self._catalog_manager.get_catalog_name()
        namespace = self._schema_manager.get_stage_namespace(stage)
        return create_table_fullname(catalog, namespace, table_name)

    def get_table_metadata(
        self,
        table_ref: str,
        stage: StageType,
        query_params: Optional[Dict[str, str]] = None,
    ) -> TableMetadata:
        if stage is None:
            raise ValueError("Stage must be provided to get table metadata.")

        catalog = self._catalog_manager.get_catalog_name()
        namespace = self._schema_manager.get_stage_namespace(stage)
        upstream_stage = self._schema_manager.get_stage_upstream(stage)

        target_fullname = self.get_table_fullname(table_ref, stage)
        target_schema = self.get_table_schema(table_ref, stage)
        write_mode = self.get_table_write_mode(table_ref, stage)
        queries = self.get_formated_query(table_ref, **(query_params or {}))

        common_kwargs = {
            "name": table_ref,
            "catalog": catalog,
            "target_fullname": target_fullname,
            "target_schema": target_schema,
            "write_mode": write_mode,
            "queries": queries,
        }

        if namespace in ("bronze", "silver"):
            return BronzeSilverTableMetadata(
                **common_kwargs,
                namespace=namespace,
                source_fullname=self.get_table_fullname(table_ref, upstream_stage),
                source_schema=self.get_table_schema(table_ref, upstream_stage),
            )

        return GoldTableMetadata(
            **common_kwargs,
            namespace=namespace,
        )

if __name__ == "__main__":
    from pprint import pprint
    table_manager = TableManager().get_table_schema("cancellation_summary", "bronze")
    print(table_manager)

