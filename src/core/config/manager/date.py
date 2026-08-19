from datetime import datetime

from src.core.config.config import Config
from src.models.data_config import (
    DateConfig
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

if __name__ == "__main__":
    date_manager = DateManager()
    print("Start Date:", date_manager.get_start_date())
    print("End Date:", date_manager.get_end_date())