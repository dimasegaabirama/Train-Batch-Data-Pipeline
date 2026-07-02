from logging import Logger

from pyspark.sql import SparkSession
from typing_extensions import Literal

from src.core import Config

from src.models.spark_config import SparkLayerContext
from src.models.data_config import StageType


class Session:
    def __init__(self, logger: Logger, config: Config):
        self.logger = logger
        self.config = config
        self.spark_cfg = self.config.get_spark_config()

    def get_stage_config(self, stage: StageType) -> SparkLayerContext:
        """Get Spark configuration for a specific stage."""
        try:
            return getattr(self.spark_cfg, stage)
        except AttributeError as err:
            raise ValueError(f"Invalid stage: {stage}") from err

    def get_session(self, stage: StageType) -> SparkSession:
        """Create or retrieve a Spark session based on stage configuration."""
        self.logger.info(f"[Create Session] Start | Stage = {stage}")

        try:
            stage_cfg = self.get_stage_config(stage)

            builder = SparkSession.builder.appName(stage_cfg.app_name).master(
                self.spark_cfg.master
            )

            for key, value in stage_cfg.config.items():
                builder = builder.config(key, value)

            spark = builder.getOrCreate()
            spark.sparkContext.setLogLevel("ERROR")

            self.logger.info(f"[Create Session] Success | Stage = {stage}")
            return spark

        except Exception:
            self.logger.exception(f"[Create Session] Failed | Stage = {stage}")
            raise


if __name__ == "__main__":
    pass
