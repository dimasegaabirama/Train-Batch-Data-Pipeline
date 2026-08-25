from abc import ABC, abstractmethod
from typing_extensions import Dict, List, Optional

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from src.models.data_config import WriteType
from src.models.etl_config import TransformResult


class BaseLoad(ABC):
    """Base class for stage-specific loaders.

    Subclasses implement `load()` to write `self.dataframe` to its
    destination using the write configuration carried in `transform_result`.
    """

    def __init__(self, session: SparkSession, transform_result: TransformResult):
        if transform_result is None:
            raise ValueError("transform_result must be provided.")

        self.session: SparkSession = session
        self.transform_result: TransformResult = transform_result

        self.dataframe: DataFrame = self.transform_result.cleaned_dataframe
        self.write_mode: WriteType = self.transform_result.write_mode
        self.target_fullname: str = self.transform_result.target_fullname

        self.queries: List[str] = self.transform_result.queries
        self.query_params: Optional[Dict[str, str]] = self.transform_result.query_params

    @abstractmethod
    def load(self) -> None:
        """Write `self.dataframe` to its destination. Implemented by subclasses."""