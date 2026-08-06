import re
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType,
    FloatType, BooleanType, TimestampType, DateType,
    _parse_datatype_string
)

from src.models.data_config import StageType
from src.core.config.manager import TableManager

def parse_ddl_string(ddl: str) -> StructType:
    return _parse_datatype_string(f"struct<{ddl.strip()}>")

def create_table_fullname(catalog_name: str, schema_name: str, table_name: str):
    return f"{catalog_name}.{schema_name}.{table_name}"

def normalize_table_info(table_name: str | dict, table_manager: TableManager, stage: StageType) -> dict:
    if isinstance(table_name, str):
        return {
            "name": table_name,
            "fullname": table_manager.get_table_fullname(
                table_name,
                stage,
            ),
            "schema": table_manager.get_table_schema(
                table_name,
                stage,
            )
        }
    elif isinstance(table_name, dict):

        name, ctx = table_name.items()

        return {
            "name": name,
            "fullname": f'{ctx["catalog"]}.{ctx["schema"]}.{ctx["name"]}',
            "schema": table_manager.get_table_schema(
                name,
                ctx["schema"]
            )
        }
    else:
        raise ValueError(f"Invalid table_name type: {type(table_name)}. Expected str or dict.")
