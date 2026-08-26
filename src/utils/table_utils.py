from pyspark.sql.types import (
    StructType,
    _parse_datatype_string
)

def parse_ddl_string(ddl: str) -> StructType:
    return _parse_datatype_string(f"struct<{ddl.strip()}>")

def create_table_fullname(catalog_name: str, namespace_name: str, table_name: str):
    return f"{catalog_name}.{namespace_name}.{table_name}"

def create_table_view_name(table_name: str):
    return f"{table_name}_view"