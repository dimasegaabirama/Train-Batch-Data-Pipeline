from pydantic import BaseModel
from typing_extensions import Dict


class SparkLayerContext(BaseModel):
    app_name: str
    config: Dict[str, object]


class SparkConfig(BaseModel):
    master: str
    submit_image_name: str
    bootstrap: SparkLayerContext
    bronze: SparkLayerContext
    silver: SparkLayerContext
    gold: SparkLayerContext
