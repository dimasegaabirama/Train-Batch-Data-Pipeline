from abc import ABC, abstractmethod

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from src.core import Config, AppLogger


class BaseTransform(ABC):
    def __init__(
        self, logger: AppLogger, session: SparkSession, config: Config, dataframe: DataFrame, **extra
    ):
        self.logger = logger
        self.session = session
        self.config = config
        self.dataframe = dataframe

        self.extra = extra

        
    @abstractmethod
    def transform(self):
        return None
