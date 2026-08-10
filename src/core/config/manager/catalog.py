from src.core.config.config import Config
from src.models.data_config import CatalogContext, CatalogsConfig

from .pipeline import PipelineManager


class CatalogManager:
    def __init__(self):
        self._config = Config.get_config()

    def get_config(self) -> CatalogsConfig:
        return self._config.catalogs

    def get_type(self) -> str:
        pipeline = PipelineManager()
        return pipeline.get_config().catalog_type

    def get_catalog_config(self) -> CatalogContext:
        catalog_type = self.get_type()

        cfg = getattr(self.get_config(), catalog_type, None)
        if cfg is None:
            raise ValueError(f"Config for catalog type '{catalog_type}' not found!")
        return cfg

    def get_catalog_name(self) -> str:
        return self.get_catalog_config().name
