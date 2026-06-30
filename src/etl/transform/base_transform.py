from abc import ABC, abstractmethod

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from src.core.config import Config
from src.core.logger import AppLogger


class BaseTransform(ABC):
    def __init__(
        self, logger: AppLogger, session: SparkSession, config: Config, dataframe: DataFrame, **kwargs
    ):
        self.logger = logger
        self.session = session
        self.config = config
        self.dataframe = dataframe

    @abstractmethod
    def requires(self):
        """
        Returns any dependencies required before this transform runs.

        Returns
        -------
        None
        """
        return None

    @abstractmethod
    def transform(self):
        return None
