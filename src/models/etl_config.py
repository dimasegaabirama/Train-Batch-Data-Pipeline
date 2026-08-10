from pydantic import BaseModel
from typing_extensions import Dict, List, Optional

from src.models.data_config import WriteType

# =========================
# Base (shared contract)
# =========================
class PipelineResult(BaseModel):
    name: str
    fullname: str
    write_mode: WriteType
    queries: List[str] = []


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
            write_mode=extract.write_mode,
            queries=extract.queries,
            cleaned_dataframe=cleaned_dataframe,
        )


# =========================
# Load
# =========================
class LoadResult(PipelineResult):
    loaded_dataframe: object

    @classmethod
    def from_transform(cls, transform: TransformResult, loaded_dataframe: object) -> "LoadResult":
        return cls(
            name=transform.name,
            fullname=transform.fullname,
            write_mode=transform.write_mode,
            queries=transform.queries,
            loaded_dataframe=loaded_dataframe,
        )
