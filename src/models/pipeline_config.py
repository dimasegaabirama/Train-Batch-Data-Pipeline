from pydantic import BaseModel, PositiveInt
from typing_extensions import Dict, Literal, Optional, List

from src.models.data_config import StageType


FlowKey = Literal["source", "target"]


class PipelineFlow(BaseModel):
    bronze: Dict[FlowKey, StageType]
    silver: Dict[FlowKey, StageType]
    gold: Dict[FlowKey, StageType]


class PipelineConfig(BaseModel):
    name: str
    source_type: str
    catalog_type: str
    start_date: str
    schedule: str
    timezone: str
    config_host_path: str
    env_host_path: str
    stages: List
    tablenames: Dict[StageType, List[str]]
