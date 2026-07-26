from abc import ABC, abstractmethod

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from src.core import AppLogger


class BaseTransform(ABC):
    def __init__(
        self, session: SparkSession, dataframe: DataFrame, lookup_tables = None
    ):
        self.session = session
        self.dataframe = dataframe

        self.lookup_tables = lookup_tables

        
    @abstractmethod
    def transform(self):
        return None
