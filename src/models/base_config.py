from pydantic import BaseModel

from .data_config import (
    CatalogsConfig,
    DateConfig,
    FilterConfig,
    SchemasConfig,
    SourcesConfig,
    StoragesConfig,
    TablesConfig
)
from .pipeline_config import PipelineConfig
from .spark_config import SparkConfig
from .checks_config import QualityConfig


class BaseConfig(BaseModel):
    version: str
    run_date: DateConfig
    pipeline: PipelineConfig
    sources: SourcesConfig
    storages: StoragesConfig
    catalogs: CatalogsConfig
    spark: SparkConfig
    schemas: SchemasConfig
    tables: TablesConfig
    filters: FilterConfig
    data_quality: QualityConfig