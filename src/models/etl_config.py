from pydantic import BaseModel
from typing_extensions import Dict, List, Optional

from src.models.data_config import WriteType

# =========================
# Base (shared contract)
# =========================
class PipelineResult(BaseModel):
    stage: str
    name: str
    source_fullname: str
    target_fullname: str
    write_mode: WriteType
    target_schema: Optional[str] = None
    queries: List[str] = []
    query_params: Optional[Dict[str, str]] = None


# =========================
# Extract
# =========================
class ExtractResult(PipelineResult):
    catalog: str
    namespace: str
    dataframe: Optional[object] = None
    dependencies: Optional[Dict[str, object]] = None


# =========================
# Transform
# =========================
class TransformResult(PipelineResult):
    cleaned_dataframe: object

    @classmethod
    def from_extract(cls, extract: ExtractResult, cleaned_dataframe: object) -> "TransformResult":
        return cls(
            stage=extract.stage,
            name=extract.name,
            source_fullname=extract.source_fullname,
            target_fullname=extract.target_fullname,
            write_mode=extract.write_mode,
            target_schema=extract.target_schema,
            queries=extract.queries,
            query_params=extract.query_params,
            cleaned_dataframe=cleaned_dataframe
        )

