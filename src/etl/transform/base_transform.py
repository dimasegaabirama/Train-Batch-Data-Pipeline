from typing_extensions import Dict, Optional
from abc import ABC, abstractmethod

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from src.core import AppLogger


class BaseTransform(ABC):
    def __init__(
        self, session: SparkSession, table_name: str, inputs: Optional[Dict[str, DataFrame]] = None
    ):
        self.session = session
        self.table_name = table_name
        self.dataframe = inputs[table_name]
        if not self.dataframe:
            raise ValueError(f"DataFrame for table '{table_name}' is empty or not provided.")
        
        self.inputs = inputs

    @abstractmethod
    def transform(self):
        return None
