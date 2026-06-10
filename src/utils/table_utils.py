import re
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, DoubleType,
    FloatType, BooleanType, TimestampType, DateType,
    _parse_datatype_string
)

def parse_ddl_string(ddl: str) -> StructType:
    return _parse_datatype_string(f"struct<{ddl.strip()}>")

def create_table_fullname(catalog_name: str, schema_name: str, table_name: str):
    return f"{catalog_name}.{schema_name}.{table_name}"
