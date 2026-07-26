from typing_extensions import Optional, Union
from abc import ABC, abstractmethod
from src.models.data_config import StageType

from pyspark.sql.session import SparkSession
from pyspark.sql.column import Column
from src.core import Session, AppLogger, TableManager, SourceManager, SchemaManager


class BaseExtract(ABC):

    SOURCE_TYPE: Optional[str] = None

    def __init__(
        self,
        stage: StageType,
        session: SparkSession,
        table_name: str,
        condition: Optional[Union[str, Column]] = None
    ):
        self._table_manager = TableManager()
        self._source_manager = SourceManager()
        self._schema_manager = SchemaManager()

        self.stage = stage
        self.upstream_stage = self._schema_manager.get_stage_upstream(self.stage)
        self.downstream_stage = self._schema_manager.get_stage_downstream(self.stage)
        
        self.session = session
        self.condition = condition

        self.table_name = table_name
        self.table_fullname = self._table_manager.get_table_fullname(table_name, self.upstream_stage)
        self.table_schema = self._table_manager.get_table_schema(table_name, self.upstream_stage)

        if self.SOURCE_TYPE:
            self.source_config = self._source_manager.get_source_config(
                self.SOURCE_TYPE
            )
        else:
            self.source_config = None


    @abstractmethod
    def extract(self):
        pass
