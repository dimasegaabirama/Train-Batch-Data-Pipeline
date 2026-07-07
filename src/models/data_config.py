from datetime import datetime

from pydantic import BaseModel, PositiveInt, field_validator, model_validator
from typing_extensions import Dict, List, Literal, Optional

# =========================
# Stage Type
# =========================

StageType = Literal["source", "bronze", "silver", "gold"]

# =========================
# Write Type
# =========================

WriteType = Literal["append", "overwrite", "overwrite_partitions", "custom"]


# =========================
# Catalog
# =========================


class CatalogContext(BaseModel):
    name: str
    host: str
    port: int
    uri: str


class NessieContext(CatalogContext):
    name: Optional[str] = "nessie"
    ref: Optional[str] = "main"
    warehouse: str


class CatalogsConfig(BaseModel):
    nessie: NessieContext


# =========================
# Date
# =========================


class DateConfig(BaseModel):
    start_date: datetime
    end_date: datetime

    @field_validator("start_date", "end_date", mode="before")
    @classmethod
    def parse_date(cls, value: str) -> datetime:
        try:
            return datetime.strptime(value, "%Y-%m-%d")
        except ValueError:
            raise ValueError(f"Invalid date format: '{value}'. Expected YYYY-MM-DD")

    @model_validator(mode="after")
    def validate_date_range(self) -> "DateConfig":
        if self.start_date > self.end_date:
            raise ValueError(
                f"start_date ({self.start_date.date()}) must not be greater than "
                f"end_date ({self.end_date.date()})"
            )
        return self


# =========================
# Filter
# =========================


class FilterContext(BaseModel):
    type: Literal["mongo", "iceberg"]
    tables: Dict[str, Dict[str, str]]


class FilterConfig(BaseModel):
    bronze: FilterContext
    silver: FilterContext


# =========================
# Schema
# =========================

class SchemaContext(BaseModel):
    name: str
    description: str
    owner: str
    upstream: StageType
    downstream: StageType
    retention_days: PositiveInt


class SchemasConfig(BaseModel):
    bronze: SchemaContext
    silver: SchemaContext
    gold: SchemaContext


# =========================
# Source
# =========================


class SourceContext(BaseModel):
    database: str
    username: str
    password: str
    host: str
    port: int
    uri: str


class MongoConfig(SourceContext):
    replica_set: Optional[str] = None
    auth_source: Optional[str] = "admin"


class SourcesConfig(BaseModel):
    mongo: MongoConfig


# =========================
# Storage
# =========================


class StorageContext(BaseModel):
    host: str
    port: int
    uri: str


class HdfsConfig(StorageContext):
    host: Optional[str] = "namenode"
    port: Optional[int] = 9870


class StoragesConfig(BaseModel):
    hdfs: HdfsConfig


# =========================
# Tables
# =========================


class TableContext(BaseModel):
    type: Literal["scd1", "scd2", "fact"]
    partitioned_by: str
    write_mode: Dict[StageType, WriteType]
    schema: Dict[StageType, str]
    query: List[str]
    depends_on: Optional[dict] = None


class TablesConfig(BaseModel):
    passengers: TableContext
    routes: TableContext
    stations: TableContext
    trains: TableContext
    tickets: TableContext


class TableNames(BaseModel):
    names: List[Literal["passengers", "routes", "stations", "trains", "tickets"]]


