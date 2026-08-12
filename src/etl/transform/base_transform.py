from logging import Logger

from typing_extensions import List

from typing_extensions import Dict, Optional
from abc import ABC, abstractmethod

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from src.models.data_config import StageType
from src.models.etl_config import ExtractResult, TransformResult


class BaseTransform(ABC):
    def __init__(
        self, 
        stage: StageType,
        session: SparkSession,
        extract_result: ExtractResult
    ):
        if extract_result is None:
            raise ValueError("extract_result must be provided.")
        
        self.session = session
        self.extract_result = extract_result

        self.dataframe = self.validate_dataframe(stage, self.extract_result.dataframe)

        self.dependencies = self.extract_result.dependencies

    def validate_dataframe(self, stage: StageType, dataframe: Optional[DataFrame]):
        if dataframe is None and stage in ["silver", "bronze"]:
            raise ValueError(f"{stage.capitalize()} stage requires a non-empty dataframe from the extract result.")

        return dataframe
        

    @abstractmethod
    def transform(self) -> TransformResult:
        pass
