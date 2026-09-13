from src.core.config.config import Config
from src.models.pipeline_config import PipelineConfig


class PipelineManager:
    def __init__(self):
        self._config = Config.get_config()

    def get_config(self) -> PipelineConfig:
        return self._config.pipeline

if __name__ == "__main__":
    pipeline_manager = PipelineManager()
    pipeline_config = pipeline_manager.get_config()
    print(pipeline_config.config_host_path)
    print(pipeline_config.env_host_path)
    