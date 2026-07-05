from abc import ABC, abstractmethod
from logging import Logger
from typing_extensions import Union, Optional
from src.models.data_config import StageType

from src.core import Config, Session, AppLogger
from src.utils.table_utils import create_table_fullname


class BaseExtract(ABC):
    def __init__(
        self,
        stage: StageType,
        logger: AppLogger,
        session: Session,
        config: Config,
        table_name: str,
        condition = None,
        table_schema = None,
        **extra
    ):
        self.stage = stage
        self.logger = logger
        self.session = session
        self.config = config
        self.table_name = table_name
        self.condition = condition
        self.table_schema = table_schema

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
    def extract(self):
        pass
