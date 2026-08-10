from src.core.config.config import Config
from src.models.data_config import (
    SchemaContext,
    SchemasConfig,
    StageType,
)


class SchemaManager:
    def __init__(self):
        self._config = Config.get_config()

    def get_config(self) -> SchemasConfig:
        return self._config.schemas

    def get_stage_config(self, stage: StageType) -> SchemaContext:
        cfg = getattr(self.get_config(), stage, None)
        if cfg is None:
            raise ValueError(f"Schema config for stage '{stage}' not found")
        return cfg

    def get_stage_schema_name(self, stage: StageType) -> str:
        return self.get_stage_config(stage).name

    def get_stage_upstream(self, stage: StageType) -> StageType:
        return self.get_stage_config(stage).upstream

    def get_stage_downstream(self, stage: StageType) -> StageType:
        return self.get_stage_config(stage).downstream
