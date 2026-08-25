from pydantic import BaseModel, model_validator
from pyspark.sql import DataFrame
from typing_extensions import Dict, List, Optional

from src.models.data_config import StageType, WriteType


class PipelineResult(BaseModel):
    stage: str
    name: str
    source_fullname: str
    target_fullname: str
    write_mode: WriteType
    target_schema: Optional[str] = None
    queries: List[str] = []
    query_params: Optional[Dict[str, str]] = None

    @model_validator(mode="after")
    def check_source_fullname(self) -> None:
        if self.stage in ("bronze", "silver") and self.source_fullname is None:
            raise ValueError(
                f"Source fullname is required for stage '{self.stage}' and table '{self.name}'."
            )


class ExtractResult(PipelineResult):
    catalog: str
    namespace: str
    dataframe: Optional[object] = None
    dependencies: Optional[Dict[str, object]] = None
    extract_main: bool = True

    @model_validator(mode="after")
    def validate_deps_and_main_table(self):
        if not self.extract_main and not self.dependencies:
            raise ValueError(
                f"Cannot extract table '{self.name}': extract_main is set to False, "
                f"but no dependencies are defined for this table."
            )

    @model_validator(mode="after")
    def validate_dataframe(self) -> None:

        if self.dataframe is None and self.stage in ["silver", "bronze"]:
            raise ValueError(
                f"{self.stage.capitalize()} stage requires a non-empty dataframe "
                f"from the extract result."
            )

        return None


class TransformResult(PipelineResult):
    cleaned_dataframe: object
    view_name: Optional[str] = None
    
    @model_validator(mode="after")
    def resolve_view_name(self):
        if self.write_mode == "custom" and self.view_name is None:
            self.view_name = f"{self.name}_view"
        return self

    @classmethod
    def from_extract(cls, extract: ExtractResult, cleaned_dataframe: object, view_name: Optional[str] = None) -> "TransformResult":
        return cls(
            stage=extract.stage,
            name=extract.name,
            source_fullname=extract.source_fullname,
            target_fullname=extract.target_fullname,
            write_mode=extract.write_mode,
            target_schema=extract.target_schema,
            queries=extract.queries,
            query_params=extract.query_params,
            cleaned_dataframe=cleaned_dataframe,
            view_name=view_name
        )

