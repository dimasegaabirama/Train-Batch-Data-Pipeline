from src.core.config import Config
from src.models.data_config import (
    SourceContext,
    SourcesConfig,
)

from .pipeline import PipelineManager


class SourceManager:
    def __init__(self):
        self._config = Config.get_config()

    def get_config(self) -> SourcesConfig:
        return self._config.sources

    def get_type(self) -> str:
        pipeline = PipelineManager()
        return pipeline.get_config().source_type

    def get_source_config(self, source_name: str) -> SourceContext:
        cfg = getattr(self.get_config(), source_name, None)
        if cfg is None:
            raise ValueError(f"Source config for '{source_name}' not found")
        return cfg
