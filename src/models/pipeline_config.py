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
    schedule: str
    start_date: str
    config_path: str
    env_path: str
    stages: List
    tablenames: Dict[StageType, List[str]]
