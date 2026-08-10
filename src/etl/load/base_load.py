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
        stage: StageType, 
        session: SparkSession, 
        transform_result: TransformResult,
        dataframe: DataFrame
    ):
        self.stage = stage
        self.session = session
        self.dataframe = dataframe

        self.transform_result = transform_result

    @abstractmethod
    def load(self):
        pass
