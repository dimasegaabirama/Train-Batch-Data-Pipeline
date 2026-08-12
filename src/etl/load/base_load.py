from abc import ABC, abstractmethod
from typing import List
from typing_extensions import Dict
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from src.models.etl_config import TransformResult
from src.models.data_config import StageType

class BaseLoad(ABC):

    def __init__(
        self, 
        session: SparkSession, 
        transform_result: TransformResult
    ):
        if transform_result is None:
            raise ValueError("transform_result must be provided.")
        
        self.session = session
        self.transform_result = transform_result

        self.dataframe: DataFrame = self.transform_result.cleaned_dataframe
        self.write_mode: str = self.transform_result.write_mode
        self.location: str = self.transform_result.location
        self.queries: List[str] = self.transform_result.queries


    @abstractmethod
    def load(self):
        pass
