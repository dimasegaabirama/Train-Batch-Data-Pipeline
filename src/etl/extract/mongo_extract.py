from typing_extensions import List, Dict, Optional

from src.models.etl_config import ExtractResult
from src.utils.table_utils import create_table_fullname
from .base_extract import BaseExtract

class MongoExtract(BaseExtract):

    SOURCE_TYPE = "mongo"

    def extract(self, extract_main: Optional[bool] = True) -> ExtractResult:

        database = self.source_config.database
        deps_table: Dict[str, "DataFrame"] = {}

        try:
            dependencies = self.table_deps[self.table_name]
            for dep in dependencies:
                deps_table[dep.name] = self._read_collection(
                    database, dep.name
                )

            if not extract_main and not deps_table:
                raise ValueError(
                    f"Cannot extract table '{self.table_name}': extract_main is set to False, "
                    f"but no dependencies are defined for this table."
                )

            df = self._read_collection(database, self.table_name) if extract_main else None

            return ExtractResult(
                name=self.table_name,
                catalog=self.main_table.catalog,
                schema_name=self.main_table.schema_name,
                fullname=self.main_table.fullname,
                location=self.main_table.location,
                write_mode=self.main_table.write_mode,
                dataframe=df,
                queries=self.main_table.queries,
                query_params=self.main_table.query_params,
                dependencies=deps_table
            )
        
        except Exception as e:
            raise RuntimeError(f"Failed to extract data for table '{self.table_name}': {e}") from e


    def _read_collection(self, database: str, table: str):
        schema = self.main_table.schema
        if not schema:
            raise ValueError(f"Schema not found for table '{table}'")

        reader = (
            self.session.read.format("mongodb")
            .option("database", database)
            .option("collection", table)
            .schema(schema)
        )

        condition = self._resolve_condition(table)
        if condition:
            reader = reader.option("aggregation.pipeline", condition)

        return reader.load()


    def _resolve_condition(self, table: str):
        if table == self.table_name:
            return self.condition
        return None  


if __name__ == "__main__":
    pass
