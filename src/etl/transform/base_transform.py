from abc import ABC, abstractmethod

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from src.core.config import Config


class BaseTransform(ABC):
    def __init__(
        self, session: SparkSession, config: Config, dataframe: DataFrame, **kwargs
    ):
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
