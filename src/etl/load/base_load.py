from abc import ABC, abstractmethod
from typing_extensions import Dict
from pyspark.sql.dataframe import DataFrame


from src.core import Session, AppLogger, TableManager
from src.models.data_config import StageType

class BaseLoad(ABC):

    def __init__(self, stage: StageType, logger: AppLogger, session: Session, table_name: str, dataframe: DataFrame, query_params: Dict[str, str], **kwargs):
        self.stage = stage
        self.logger = logger
        self.session = session
        self.dataframe = dataframe
        
        self._table_manager = TableManager()
        self.table_name = table_name
        self.table_fullname = self._table_manager.get_table_fullname(table_name, stage)
        self.write_mode = self._table_manager.get_table_write_mode(table_name, stage)
        self.queries = self._table_manager.get_formated_query(table_name, **query_params)

    @abstractmethod
    def load(self):
        pass
