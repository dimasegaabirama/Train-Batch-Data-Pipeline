import os
import yaml
from dotenv import load_dotenv
from typing_extensions import List
from pprint import pprint

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
    TablesConfig
)
from src.models.pipeline_config import PipelineConfig
from src.models.spark_config import SparkConfig
from src.utils.table_utils import create_table_fullname
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
    # Core Loader
    # =========================

    @classmethod
    def get_config(cls) -> BaseConfig:
        """Load and cache configuration from YAML + env vars."""

        load_dotenv(".env.global")
        
        env_path = os.getenv("ENV_PATH")
        config_path = os.getenv("CONFIG_PATH")

        if cls._config is None:
            load_dotenv(env_path)
            with open(config_path) as f:
                raw = yaml.safe_load(f)
            cls._config = BaseConfig(**replace_env(raw))
        return cls._config

    # =========================
    # Run Date
    # =========================

    @classmethod
    def get_run_date(cls) -> DateConfig:
        return cls.get_config().run_date

    @classmethod
    def get_start_date(cls) -> str:
        return cls.get_run_date().start_date

    @classmethod
    def get_end_date(cls) -> str:
        return cls.get_run_date().end_date

    # =========================
    # Tables
    # =========================

    @classmethod
    def get_all_tables_config(cls) -> TablesConfig:
        return cls.get_config().tables

    @classmethod
    def get_all_tablenames(cls) -> List[str]:
        return getattr(cls.get_pipeline_config(), "tablenames")

    @classmethod
    def get_table_config(cls, table_name: str) -> TableContext:
        cfg = getattr(cls.get_all_tables_config(), table_name, None)
        if cfg is None:
            raise ValueError(f"Table config '{table_name}' not found")
        return cfg

    @classmethod
    def get_table_type(cls, table_name: str) -> str:
        cfg = getattr(cls.get_table_config(table_name), "type", None)
        if cfg is None:
            raise ValueError(f"Table type '{table_name}' not found")
        return cfg

    @classmethod
    def get_schema_table(self, table_name: str) -> str:
        cfg = getattr(self.config.get_table_config(table_name), "schema", None)
        if not cfg:
            raise ValueError(f"Schema for '{table_name}' is not found!")
        return cfg
    
    # =========================
    # Catalogs
    # =========================

    @classmethod
    def get_all_catalogs_config(cls) -> CatalogsConfig:
        return cls.get_config().catalogs

    @classmethod
    def get_catalog_config(cls) -> CatalogContext:

        catalog_type = cls.get_catalog_type()

        cfg = getattr(cls.get_all_catalogs_config(), catalog_type, None)
        if cfg is None:
            raise ValueError(f"Catalog config '{catalog_type}' not found")
        return cfg

    @classmethod
    def get_catalog_type(cls) -> str:
        cfg = cls.get_pipeline_config().catalog_type
        if cfg is None:
            raise ValueError(f"Catalog Type must be required!!")
        return cfg
    
    @classmethod
    def get_catalog_name(cls) -> str:
        return cls.get_catalog_config().name

    # =========================
    # Pipeline
    # =========================

    @classmethod
    def get_pipeline_config(cls) -> PipelineConfig:
        return cls.get_config().pipeline

    # =========================
    # Sources
    # =========================

    @classmethod
    def get_all_sources_config(cls) -> SourcesConfig:
        return cls.get_config().sources

    @classmethod
    def get_source_config(cls, source_name: str) -> SourceContext:
        cfg = getattr(cls.get_all_sources_config(), source_name, None)
        if cfg is None:
            raise ValueError(f"Source config '{source_name}' not found")
        return cfg

    # =========================
    # Storages
    # =========================

    @classmethod
    def get_all_storages_config(cls) -> StoragesConfig:
        return cls.get_config().storages

    @classmethod
    def get_storage_config(cls, storage_name: str) -> StorageContext:
        cfg = getattr(cls.get_all_storages_config(), storage_name, None)
        if cfg is None:
            raise ValueError(f"Storage config '{storage_name}' not found")
        return cfg

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
        cfg = getattr(cls.get_all_schemas_config(), stage, None)
        if cfg is None:
            raise ValueError(f"Schema config for stage '{stage}' not found")
        return cfg

    @classmethod
    def get_schema_name(cls, stage: StageType) -> str:
        cfg = getattr(cls.get_schema_config(stage), "name", None)
        if cfg is None:
            raise ValueError(f"Schema name for stage '{stage}' not found")
        return cfg

    # =========================
    # Filters
    # =========================

    @classmethod
    def get_all_filters_config(cls) -> FilterConfig:
        return cls.get_config().filters

    @classmethod
    def get_filter_config(cls, stage: StageType) -> "FilterContext | None":
        """
        Return filter config for a stage.

        Returns None when the stage does not define filters.
        """
        return getattr(cls.get_all_filters_config(), stage, None)

    @classmethod
    def get_filter_field(cls, stage: StageType, table_name: str) -> "str | None":
        """
        Return filter field for a table.

        Returns None when:
        - stage has no filter config
        - table has no filter config
        """
        cfg = cls.get_filter_config(stage)
        if cfg is None:
            return None

        tbl_cfg = getattr(cfg, table_name, None)
        if tbl_cfg is None:
            return None

        return tbl_cfg.field



if __name__ == "__main__":

    from src.core.session import Session
    from src.core.logger import AppLogger

    logger = AppLogger.get_logger()
    conf = Config()

    configs = conf.get_table_config("passengers").query
    for x in configs:
        print(x)

    




    

    