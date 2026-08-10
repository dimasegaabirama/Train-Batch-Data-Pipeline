from abc import ABC, abstractmethod
from typing import List
from typing_extensions import Dict
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from src.core import Session, AppLogger, TableManager
from src.models.data_config import StageType, TableMetadata

class BaseLoad(ABC):

    def __init__(
        self, 
        stage: StageType, 
        session: SparkSession, 
        metadata_tables: List[TableMetadata],
        dataframe: DataFrame
    ):
        self.stage = stage
        self.session = session
        self.dataframe = dataframe
        
        self._table_manager = TableManager()
        self.metadata_tables = metadata_tables

    @abstractmethod
    def load(self):
        pass
