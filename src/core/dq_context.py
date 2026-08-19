from typing import Optional

from pyspark.sql import SparkSession
from src.models.etl_config import TransformResult

from pyspark.sql.dataframe import DataFrame

class DataQualityContext:
    """
    Shared in-process context untuk oper TransformResult dari
    PipelineOrchestrator ke pytest BaseTest, tanpa lewat disk/SQL view.
    Aman karena pytest.main() dijalankan in-process (sama interpreter,
    sama SparkContext).
    """
    _transform_result: Optional[TransformResult] = None
    _session: Optional[SparkSession] = None
    _dataframe: Optional[DataFrame] = None

    @classmethod
    def set(cls, session: SparkSession, transform_result: TransformResult, dataframe: DataFrame) -> None:
        cls._transform_result = transform_result
        cls._session = session
        cls._dataframe = dataframe

    @classmethod
    def get_transform_result(cls) -> Optional[TransformResult]:
        return cls._transform_result

    @classmethod
    def get_session(cls) -> Optional[SparkSession]:
        return cls._session

    @classmethod
    def get_dataframe(cls) -> Optional[DataFrame]:
        return cls._dataframe

    @classmethod
    def clear(cls) -> None:
        cls._transform_result = None
        cls._session = None
        cls._dataframe = None