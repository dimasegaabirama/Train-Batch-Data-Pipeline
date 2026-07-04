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
    catalog_type: str
    stages: List
    tablenames: List
