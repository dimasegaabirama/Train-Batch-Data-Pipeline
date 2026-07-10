from src.core.config.config import Config
from src.models.spark_config import SparkConfig, SparkLayerContext
from src.models.data_config import StageType


class SparkManager:
    def __init__(self):
        self._config = Config.get_config()

    def get_config(self) -> SparkConfig:
        return self._config.spark
    
    def get_stage_config(self, stage: StageType) -> SparkLayerContext:
        cfg = getattr(self.spark_cfg, stage)
        if cfg is None:
            raise ValueError(f"Spark config for stage '{stage}' not found")
        return cfg
