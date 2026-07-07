from datetime import datetime

from src.core.config import Config
from src.models.data_config import (
    DateConfig,
)


class DateManager:
    def __init__(self):
        self._config = Config.get_config()

    def get_date(self) -> DateConfig:
        return self._config.run_date

    def get_start_date(self) -> datetime:
        return self.get_date().start_date

    def get_end_date(self) -> datetime:
        return self.get_date().end_date
