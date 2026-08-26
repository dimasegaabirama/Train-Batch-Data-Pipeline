from typing_extensions import Optional

from src.core.config.config import Config
from src.models.data_config import (
    FiltersConfig,
    StageFilters,
    StageType
)


class FilterManager:
    def __init__(self):
        self._config = Config.get_config()

    def get_config(self) -> FiltersConfig:
        return self._config.filters

    def get_stage_config(self, stage: StageType) -> Optional[StageFilters]:
        return getattr(self.get_config(), stage, None)


    def get_fields(self, stage: StageType, table_name: str) -> "str | None":
        cfg = self.get_stage_config(stage)
        if cfg is None:
            return None

        return cfg.tables.get(table_name, [])

if __name__ == "__main__":
    filter_manager = FilterManager()
    stage = "bronze"
    table_name = "passengers"
    field = filter_manager.get_field(stage, table_name)
    print(field)
