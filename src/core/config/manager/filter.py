from src.core.config.config import Config
from src.models.data_config import (
    FilterConfig,
    FilterContext,
    StageType,
)


class FilterManager:
    def __init__(self):
        self._config = Config.get_config()

    def get_config(self) -> FilterConfig:
        return self._config.filters

    def get_stage_config(self, stage: StageType) -> "FilterContext | None":
        return getattr(self.get_config(), stage, None)


    def get_field(self, stage: StageType, table_name: str) -> "str | None":
        cfg = self.get_stage_config(stage)
        if cfg is None:
            return None

        table_filter = cfg.tables.get(table_name)
        if table_filter is None:
            return None

        return table_filter.get("field")
