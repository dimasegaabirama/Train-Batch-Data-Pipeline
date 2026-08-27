from typing_extensions import Optional, List, Dict

from src.core.config.config import Config
from src.models.data_config import (
    FiltersConfig,
    FilterField,
    StageFilters,
    StageType
)


#   silver:
#     type: iceberg
#     tables:
#       tickets:
#         - table: tickets
#           field: created_at
#           strategy: incremental

#         - table: passengers
#           field: is_active
#           strategy: boolean

#         - table: routes
#           field: is_deleted
#           strategy: boolean

#         - table: trains
#           field: is_active
#           strategy: boolean

class FilterManager:
    def __init__(self):
        self._config = Config.get_config()

    def get_config(self) -> FiltersConfig:
        return self._config.filters

    def get_stage_config(self, stage: StageType) -> Optional[StageFilters]:
        cfg = getattr(self.get_config(), stage, None)
        if cfg is None:
            raise ValueError(f"Filter config for stage '{stage}' not found")
        return cfg

    def get_stage_type(self, stage: StageType) -> "str | None":
        cfg = self.get_stage_config(stage)
        return cfg.type

    def get_table_filters(self, stage: StageType, table_name: str) -> "str | None":
        cfg = self.get_stage_config(stage)
        cfg_table = cfg.tables.get(table_name, None)
        if cfg_table is None:
            raise ValueError(f"Filter config for table '{table_name}' in stage '{stage}' not found")
        return cfg_table

    def get_table_filters(self, stage: StageType, table_name: str) -> List[FilterField]:
        cfg = self.get_stage_config(stage)
        cfg_table = cfg.tables.get(table_name, None)
        if cfg_table is None:
            raise ValueError(f"Filter config for table '{table_name}' in stage '{stage}' not found")
        return cfg_table

if __name__ == "__main__":
    filter_manager = FilterManager()
    stage = "bronze"
    table_name = "passengers"
    stage_type = filter_manager.get_table_filters("silver", "routes")
    print(stage_type)
