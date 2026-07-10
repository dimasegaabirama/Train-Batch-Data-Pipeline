from typing_extensions import Optional
from abc import ABC, abstractmethod
from src.models.data_config import StageType

from src.core import Session, AppLogger, TableManager, SourceManager


class BaseExtract(ABC):

    SOURCE_TYPE: Optional[str] = None

    def __init__(
        self,
        stage: StageType,
        logger: AppLogger,
        session: Session,
        table_name: str,
        condition = None
    ):
        self.stage = stage
        self.logger = logger
        self.session = session
        self.condition = condition
        
        self._table_manager = TableManager()
        self._source_manager = SourceManager()

        self.table_name = table_name
        self.table_fullname = self._table_manager.get_table_fullname(table_name, stage)
        self.table_schema = self._table_manager.get_table_schema(table_name, stage)

        if self.SOURCE_TYPE:
            self.source_config = self._source_manager.get_source_config(
                self.SOURCE_TYPE
            )
        else:
            self.source_config = None


    @abstractmethod
    def extract(self):
        pass
