from src.core.config import Config
from src.models.pipeline_config import PipelineConfig


class PipelineManager:
    def __init__(self):
        self._config = Config.get_config()

    def get_config(self) -> PipelineConfig:
        return self._config.pipeline
