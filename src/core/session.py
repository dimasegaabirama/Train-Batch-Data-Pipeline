from logging import Logger
from typing import Optional

from pyspark.sql import SparkSession

from src.core import SparkManager
from src.models.data_config import StageType


class Session:
    def __init__(self, logger: Logger, stage: StageType):
        self.stage = stage
        self.logger = logger
        self._spark_manager = SparkManager()
        self._config = self._spark_manager.get_config()
        self._stage_config = self._spark_manager.get_stage_config(self.stage)
        self._session: Optional[SparkSession] = None

    def get_session(self) -> SparkSession:

        if self._session is not None:
            self.logger.info(
                f"[Create Session] Reusing existing session | Stage = {self.stage}"
            )
            return self._session

        self.logger.info(f"[Create Session] Start | Stage = {self.stage}")

        try:
            builder = SparkSession.builder.appName(self._stage_config.app_name).master(
                self._config.master
            )

            for key, value in self._stage_config.config.items():
                builder = builder.config(key, str(value))

            self._session = builder.getOrCreate()
            self._session.sparkContext.setLogLevel("ERROR")

            self.logger.info(f"[Create Session] Success | Stage = {self.stage}")
            return self._session

        except Exception:
            self.logger.exception(f"[Create Session] Failed | Stage = {self.stage}")
            raise

    def stop_session(self) -> None:
        """Stop the active Spark session, if any."""
        if self._session is not None:
            self.logger.info(f"[Stop Session] Stage = {self.stage}")
            self._session.stop()
            self._session = None

    def __enter__(self) -> SparkSession:
        return self.get_session()

    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop_session()


if __name__ == "__main__":
    pass
