from abc import ABC, abstractmethod
from typing_extensions import Optional, Dict

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from src.models.data_config import StageType, TableDependency
from src.models.etl_config import ExtractResult, BronzeSilverExtractResult, GoldTransformResult, BronzeSilverTransformResult, GoldTransformResult, TransformResult


class BaseTransform(ABC):

    def __init__(
        self, 
        session: SparkSession, 
        extract_result: ExtractResult
    ):
        if extract_result is None:
            raise ValueError("extract_result must be provided.")

        self.session: SparkSession = session
        self.extract_result: ExtractResult = extract_result

        self.dataframe: Optional[DataFrame] = self.extract_result.dataframe
        self.dependencies: Dict[str, TableDependency] = self.extract_result.dependencies

    def _build_result(self, cleaned_dataframe: Optional[DataFrame]) -> TransformResult:
        if isinstance(self.extract_result, BronzeSilverExtractResult):
            return BronzeSilverTransformResult.from_extract(
                extract=self.extract_result,
                cleaned_dataframe=cleaned_dataframe
            )

        return GoldTransformResult.from_extract(
            extract=self.extract_result,
            cleaned_dataframe=cleaned_dataframe
        )

    @abstractmethod
    def transform(self) -> TransformResult:
        pass