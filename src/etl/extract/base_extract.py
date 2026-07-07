from abc import ABC, abstractmethod
from logging import Logger
from typing_extensions import Union, Optional
from src.models.data_config import StageType

from src.core import Config, Session, AppLogger


class BaseExtract(ABC):
    def __init__(
        self,
        stage: StageType,
        logger: AppLogger,
        session: Session,
        config: Config,
        table_name: str,
        condition = None,
        **extra
    ):
        self.stage = stage
        self.logger = logger
        self.session = session
        self.config = config
        self.table_name = table_name
        self.condition = condition
        self.table_schema = self.config.get_schema_table(table_name=table_name, stage=stage)

        self.extra = extra


    @abstractmethod
    def extract(self):
        pass
