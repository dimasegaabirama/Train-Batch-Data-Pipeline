from abc import ABC, abstractmethod
from logging import Logger
from typing_extensions import Optional, Union, List

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from src.core import Session, AppLogger, Config
from src.models.data_config import WriteType, StageType

from src.utils.table_utils import create_table_fullname

class BaseLoad(ABC):

    def __init__(self, stage: StageType, logger: AppLogger, session: Session, config: Config, table_name: str, queries: Optional[Union[str, List[str]]] = None, **extra):
        self.stage = stage
        self.logger = logger
        self.session = session
        self.config = config
        self.table_name = table_name
        self.write_mode = self.config.get_table_write_mode(table_name=table_name, stage=stage)
        
        if isinstance(queries, str):
            queries = [queries]
        self.queries = queries
        
        self.extra = extra

    @abstractmethod
    def load(self):
        pass
