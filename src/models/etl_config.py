from pydantic import BaseModel
from typing_extensions import Dict, List, Optional

from src.models.data_config import WriteType

# =========================
# Base (shared contract)
# =========================
class PipelineResult(BaseModel):
    name: str
    fullname: str
    location: str
    write_mode: WriteType
    queries: List[str] = []
    query_params: Optional[Dict[str, str]] = None


# =========================
# Extract
# =========================
class ExtractResult(PipelineResult):
    catalog: str
    schema_name: str
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
            name=extract.name,
            fullname=extract.fullname,
            location=extract.location,
            write_mode=extract.write_mode,
            queries=extract.queries,
            query_params=extract.query_params,
            cleaned_dataframe=cleaned_dataframe
        )

