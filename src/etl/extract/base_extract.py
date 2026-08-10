from typing import Optional, Union, List, Dict
from abc import ABC, abstractmethod

from pyspark.sql.session import SparkSession
from pyspark.sql.column import Column

from src.models.data_config import StageType, TableMetadata, TableDependency
from src.models.etl_config import Dependencies, ExtractResult
from src.core import TableManager, SourceManager, SchemaManager


class BaseExtract(ABC):

    SOURCE_TYPE: Optional[str] = None

    def __init__(
        self,
        stage: StageType,
        session: SparkSession,
        main_table: TableMetadata,
        table_deps: Dict[str, List[TableDependency]],
        condition: Optional[Union[str, Column]] = None,
    ):
        self._table_manager = TableManager()
        self._source_manager = SourceManager()
        self._schema_manager = SchemaManager()

        self.stage = stage
        self.session = session
        self.condition = condition

        self.main_table = main_table
        self.table_deps = table_deps

        if self.SOURCE_TYPE:
            self.source_config = self._source_manager.get_source_config(
                self.SOURCE_TYPE
            )
        else:
            self.source_config = None

    @abstractmethod
    def extract(self, extract_main: Optional[bool] = True) -> ExtractResult:
        pass