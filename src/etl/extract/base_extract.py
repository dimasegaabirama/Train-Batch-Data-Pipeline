from pyspark.sql import DataFrame
from typing_extensions import Optional, Union, List, Dict
from abc import ABC, abstractmethod
from src.models.data_config import StageType

from pyspark.sql.session import SparkSession
from pyspark.sql.column import Column
from src.core import TableManager, SourceManager, SchemaManager
from src.utils.table_utils import normalize_table_info


class BaseExtract(ABC):

    SOURCE_TYPE: Optional[str] = None

    def __init__(
        self,
        stage: StageType,
        session: SparkSession,
        table_names: Union[str, List[str], Dict[str, str]],
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

        self.table_names = table_names if isinstance(table_names, list) else [table_names]
        self.table_infos = {
            table_name: normalize_table_info(table_name, self._table_manager, self.upstream_stage)
            for table_name in self.table_names
        }

        if self.SOURCE_TYPE:
            self.source_config = self._source_manager.get_source_config(
                self.SOURCE_TYPE
            )
        else:
            self.source_config = None


    @abstractmethod
    def extract(self) -> Dict[str, DataFrame]:
        pass
