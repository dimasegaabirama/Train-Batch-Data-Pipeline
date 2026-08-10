from logging import Logger

from typing_extensions import List

from typing_extensions import Dict, Optional
from abc import ABC, abstractmethod

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from src.models.etl_config import ExtractResult, TransformResult


class BaseTransform(ABC):
    def __init__(
        self, 
        session: SparkSession, 
        logger: Logger,
        extract_result: ExtractResult
    ):
        self.session = session
        self.logger = logger
        self.extract_result = extract_result
        
        self.dataframe = self.extract_result.dataframe if self.extract_result else None
        self.dependencies = self.extract_result.dependencies if self.extract_result else None

    @abstractmethod
    def transform(self) -> TransformResult:
        return None
