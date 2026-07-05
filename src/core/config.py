import os
from datetime import datetime

import yaml
from dotenv import load_dotenv
from typing_extensions import Any, Dict, List, Union

from src.models.base_config import BaseConfig
from src.models.data_config import (
    CatalogContext,
    CatalogsConfig,
    DateConfig,
    FilterConfig,
    FilterContext,
    SchemaContext,
    SchemasConfig,
    SourceContext,
    SourcesConfig,
    StageType,
    StorageContext,
    StoragesConfig,
    TableContext,
    TablesConfig,
)
from src.models.pipeline_config import PipelineConfig
from src.models.spark_config import SparkConfig
from src.utils.text_utils import replace_env


class Config:
    """
    Singleton configuration manager.

    Loads configuration from YAML and environment variables once,
    then exposes helper methods for accessing specific config sections.
    """

    _instance: "Config | None" = None
    _config: "BaseConfig | None" = None

    # =========================
    # Singleton
    # =========================

    def __new__(cls, *args, **kwargs) -> "Config":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    # =========================
    # Internal Helpers
    # =========================

    @staticmethod
    def _require(value: Any, message: str) -> Any:
        """Return value, raising ValueError when it is None."""
        if value is None:
            raise ValueError(message)
        return value

    @staticmethod
    def _require_stage(mapping: Dict[StageType, Any], stage: StageType, message: str) -> Any:
        """Return mapping[stage], raising ValueError when the stage is missing."""
        try:
            return mapping[stage]
        except KeyError:
            raise ValueError(message) from None

    # =========================
    # Core Loader
    # =========================

    @classmethod
    def get_config(cls) -> BaseConfig:
        """Load and cache configuration from YAML + env vars."""
        if cls._config is None:
            cls._config = cls._load_config()
        return cls._config

    @classmethod
    def _load_config(cls) -> BaseConfig:
        load_dotenv(".env.global")
        load_dotenv(os.getenv("ENV_PATH"))

        config_path = cls._require(
            os.getenv("CONFIG_PATH"),
            "CONFIG_PATH environment variable is not set",
        )

        with open(config_path) as f:
            raw = yaml.safe_load(f)

        return BaseConfig(**replace_env(raw))

    # =========================
    # Run Date
    # =========================

    @classmethod
    def get_run_date(cls) -> DateConfig:
        return cls.get_config().run_date

    @classmethod
    def get_start_date(cls) -> datetime:
        return cls.get_run_date().start_date

    @classmethod
    def get_end_date(cls) -> datetime:
        return cls.get_run_date().end_date

    # =========================
    # Tables
    # =========================

    @classmethod
    def get_all_tables_config(cls) -> TablesConfig:
        return cls.get_config().tables

    @classmethod
    def get_all_tablenames(cls) -> List[str]:
        return cls.get_pipeline_config().tablenames

    @classmethod
    def get_table_config(cls, table_name: str) -> TableContext:
        return cls._require(
            getattr(cls.get_all_tables_config(), table_name, None),
            f"Table config '{table_name}' not found",
        )

    @classmethod
    def get_table_type(cls, table_name: str) -> str:
        return cls.get_table_config(table_name).type

    @classmethod
    def get_table_write_mode(cls, table_name: str, stage: StageType) -> str:
        return cls._require_stage(
            cls.get_table_config(table_name).write_mode,
            stage,
            f"Write mode for table '{table_name}' stage '{stage}' not found",
        )

    @classmethod
    def get_query_table(cls, table_name: str) -> List[str]:
        return cls.get_table_config(table_name).query

    @classmethod
    def get_schema_table(cls, table_name: str, stage: StageType) -> str:
        return cls._require_stage(
            cls.get_table_config(table_name).schema,
            stage,
            f"Schema for table '{table_name}' stage '{stage}' not found",
        )

    @classmethod
    def get_table_deps(cls, table_names: Union[str, List[str]]) -> Dict[str, str]:
        """Map each dependency table to its fully qualified name."""
        if isinstance(table_names, str):
            table_names = [table_names]

        dependencies: Dict[str, str] = {}
        for table_name in table_names:
            deps = cls.get_table_config(table_name).depends_on or {}
            dependencies.update({
                name: f"{ctx['catalog']}.{ctx['schema']}.{name}"
                for name, ctx in deps.items()
            })
        return dependencies

    # =========================
    # Catalogs
    # =========================

    @classmethod
    def get_all_catalogs_config(cls) -> CatalogsConfig:
        return cls.get_config().catalogs

    @classmethod
    def get_catalog_config(cls) -> CatalogContext:
        catalog_type = cls.get_catalog_type()
        return cls._require(
            getattr(cls.get_all_catalogs_config(), catalog_type, None),
            f"Catalog config '{catalog_type}' not found",
        )

    @classmethod
    def get_catalog_type(cls) -> str:
        return cls.get_pipeline_config().catalog_type

    @classmethod
    def get_catalog_name(cls) -> str:
        return cls.get_catalog_config().name

    # =========================
    # Pipeline
    # =========================

    @classmethod
    def get_pipeline_config(cls) -> PipelineConfig:
        return cls.get_config().pipeline

    @classmethod
    def get_pipeline_source_type(cls) -> str:
        return cls.get_pipeline_config().source_type

    # =========================
    # Sources
    # =========================

    @classmethod
    def get_all_sources_config(cls) -> SourcesConfig:
        return cls.get_config().sources

    @classmethod
    def get_source_config(cls, source_name: str) -> SourceContext:
        return cls._require(
            getattr(cls.get_all_sources_config(), source_name, None),
            f"Source config '{source_name}' not found",
        )

    # =========================
    # Storages
    # =========================

    @classmethod
    def get_all_storages_config(cls) -> StoragesConfig:
        return cls.get_config().storages

    @classmethod
    def get_storage_config(cls, storage_name: str) -> StorageContext:
        return cls._require(
            getattr(cls.get_all_storages_config(), storage_name, None),
            f"Storage config '{storage_name}' not found",
        )

    # =========================
    # Spark
    # =========================

    @classmethod
    def get_spark_config(cls) -> SparkConfig:
        return cls.get_config().spark

    # =========================
    # Schemas
    # =========================

    @classmethod
    def get_all_schemas_config(cls) -> SchemasConfig:
        return cls.get_config().schemas

    @classmethod
    def get_schema_config(cls, stage: StageType) -> SchemaContext:
        return cls._require(
            getattr(cls.get_all_schemas_config(), stage, None),
            f"Schema config for stage '{stage}' not found",
        )

    @classmethod
    def get_schema_name(cls, stage: StageType) -> str:
        return cls.get_schema_config(stage).name

    @classmethod
    def get_schema_upstream(cls, stage: StageType) -> StageType:
        return cls.get_schema_config(stage).upstream

    @classmethod
    def get_schema_downstream(cls, stage: StageType) -> StageType:
        return cls.get_schema_config(stage).downstream

    # =========================
    # Filters
    # =========================

    @classmethod
    def get_all_filters_config(cls) -> FilterConfig:
        return cls.get_config().filters

    @classmethod
    def get_filter_config(cls, stage: StageType) -> "FilterContext | None":
        """Return filter config for a stage, or None when the stage has none."""
        return getattr(cls.get_all_filters_config(), stage, None)

    @classmethod
    def get_filter_field(cls, stage: StageType, table_name: str) -> "str | None":
        """
        Return the pushdown filter field for a table.

        Returns None when the stage or the table defines no filter.
        """
        cfg = cls.get_filter_config(stage)
        if cfg is None:
            return None

        table_filter = cfg.tables.get(table_name)
        if table_filter is None:
            return None

        return table_filter.get("field")


if __name__ == "__main__":
    print(Config.get_query_table("passengers"))
