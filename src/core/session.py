from logging import Logger
from typing import Optional

from pyspark.sql import SparkSession

from src.core import SparkManager
from src.models.data_config import StageType


class Session:
    def __init__(self, stage: StageType, logger: Optional[Logger] = None):
        self.stage = stage
        self.logger = logger
        self._spark_manager = SparkManager()
        self._config = self._spark_manager.get_config()
        self._stage_config = self._spark_manager.get_stage_config(self.stage)
        self._session: Optional[SparkSession] = None


    def get_session(self) -> SparkSession:

        self.logger.info(f"Getting Spark session for stage {self.stage}")

        if self._session is not None:
            self.logger.debug(f"Using existing Spark session for stage {self.stage}")
            return self._session
        
        try:
            self.logger.debug(f"Creating new Spark session for stage {self.stage}")
            builder = SparkSession.builder.appName(self._stage_config.app_name).master(
                self._config.master
            )

            for key, value in self._stage_config.config.items():
                builder = builder.config(key, str(value))

            self._session = builder.getOrCreate()
            self._session.sparkContext.setLogLevel("ERROR")

            return self._session

        except Exception:
            raise ValueError(f"Error occurred while creating Spark session for stage {self.stage}")


    def stop_session(self) -> None:
        """Stop the active Spark session, if any."""
        if self._session is not None:
            self._session.stop()
            self._session = None


    def __enter__(self) -> SparkSession:
        return self.get_session()


    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        self.stop_session()


if __name__ == "__main__":
    pass
