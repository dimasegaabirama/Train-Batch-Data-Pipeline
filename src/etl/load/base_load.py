from abc import ABC, abstractmethod
from logging import Logger

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from src.core.config import Config
from src.models.data_config import WriteType, StageType


class BaseLoad(ABC):
    def __init__(
        self,
        logger: Logger,
        session: SparkSession,
        config: Config,
        stage: str,
        table_name: str,
        dataframe: DataFrame,
    ):
        self.logger = logger
        self.session = session
        self.config = config
        self.table_name = table_name
        self.stage = stage
        self.dataframe = dataframe

    def get_query(self) -> str | None:
        return getattr(self.config.get_table_config(self.table_name), "query", None)

    def get_write_mode(self, stage: StageType) -> WriteType:
        cfg = getattr(self.config.get_table_config(self.table_name), "write_mode", None)

        if cfg is None:
            raise ValueError(f"Write mode for table '{self.table_name}' not found")

        return cfg[stage]

    @abstractmethod
    def load(self):
        pass
