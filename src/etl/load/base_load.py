from abc import ABC, abstractmethod
from logging import Logger
from typing_extensions import Optional

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from src.core import Session, AppLogger, Config
from src.models.data_config import WriteType, StageType

from src.utils.table_utils import create_table_fullname

class BaseLoad(ABC):

    def __init__(self, stage: StageType, logger: AppLogger, session: Session, config: Config, table_name: str, write_mode: WriteType, query: Optional[str] = None, **extra):
        self.stage = stage
        self.logger = logger
        self.session = session
        self.config = config
        self.table_name = table_name
        self.write_mode = write_mode
        self.query = query
        self.extra = extra

    def resolve_full_table_name(self):
        
        catalog_name = self.config.get_catalog_name()
        schema_name = self.config.get_schema_name(stage=self.stage)

        return create_table_fullname(
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=self.table_name
        )

    @abstractmethod
    def load(self):
        pass
