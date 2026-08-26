from pydantic import BaseModel, Field, model_validator
from pyspark.sql import DataFrame
from typing_extensions import Dict, List, Optional, Union, Annotated, Literal

from src.models.data_config import StageType, WriteType


class BasePipelineResult(BaseModel):
    name: str
    catalog: str
    target_fullname: str
    write_mode: WriteType
    target_schema: str
    queries: List[str] = []


# EXTRACT

class BronzeSilverExtractResult(BasePipelineResult):
    namespace: Literal["bronze", "silver"]
    source_fullname: str
    extract_main: bool
    dataframe: object
    dependencies: Optional[Dict[str, object]] = None

    @model_validator(mode="after")
    def validate_deps_and_main_table(self):
        if not self.extract_main and not self.dependencies:
            raise ValueError(
                f"Cannot extract table '{self.name}': extract_main is set to False, "
                f"but no dependencies are defined for this table."
            )
        return self


class GoldExtractResult(BasePipelineResult):
    namespace: Literal["gold"]
    source_fullname: Optional[str] = None
    extract_main: bool = False
    dataframe: Optional[object] = None
    dependencies: Optional[Dict[str, object]] = None


ExtractResult = Annotated[
    Union[BronzeSilverExtractResult, GoldExtractResult],
    Field(discriminator="namespace"),
]


# TRANSFORM 

class BronzeSilverTransformResult(BasePipelineResult):
    namespace: Literal["bronze", "silver"]
    source_fullname: str
    cleaned_dataframe: object
    view_name: str

    @classmethod
    def from_extract(
        cls, extract: BronzeSilverExtractResult, cleaned_dataframe: object, view_name: str
    ) -> "BronzeSilverTransformResult":
        return cls(
            **extract.model_dump(exclude={"dataframe", "dependencies", "extract_main"}),
            cleaned_dataframe=cleaned_dataframe,
            view_name=view_name
        )


class GoldTransformResult(BasePipelineResult):
    namespace: Literal["gold"]
    source_fullname: Optional[str] = None
    cleaned_dataframe: object
    view_name: str

    @classmethod
    def from_extract(
        cls, extract: GoldExtractResult, cleaned_dataframe: object, view_name: str
    ) -> "GoldTransformResult":
        return cls(
            **extract.model_dump(exclude={"dataframe", "dependencies", "extract_main"}),
            cleaned_dataframe=cleaned_dataframe,
            view_name=view_name
        )


TransformResult = Annotated[
    Union[BronzeSilverTransformResult, GoldTransformResult],
    Field(discriminator="namespace"),
]
