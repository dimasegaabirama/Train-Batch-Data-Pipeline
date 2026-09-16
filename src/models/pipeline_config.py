from pydantic import BaseModel, PositiveInt
from typing_extensions import Dict, Literal, Optional, List
from src.models.data_config import PipelineStage, TableGroup


class PipelineConfig(BaseModel):
    name: str
    source_type: str
    catalog_type: str
    start_date: str
    schedule: str
    timezone: str
    config_host_path: str
    env_host_path: str
    stages: List[PipelineStage]
    tablenames: Dict[TableGroup, List[str]]
