from abc import ABC, abstractmethod
from logging import Logger
from typing_extensions import Union, Optional

from pyspark.sql.types import StructType
from pyspark.sql.session import SparkSession

from src.core.config import Config


class BaseExtract(ABC):
    def __init__(
        self,
        logger: Logger,
        session: SparkSession,
        config: Config,
        table_name: str,
        schema: Optional[Union[str, StructType]] = None,
        **kwargs,
    ):
        self.logger = logger
        self.session = session
        self.config = config
        self.table_name = table_name

        self.schema = schema

    @abstractmethod
    def extract(self):
        pass
