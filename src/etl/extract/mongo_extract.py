from pyspark.sql import DataFrame

from src.models.data_config import TableDependency

from .base_extract import BaseExtract


class MongoExtract(BaseExtract):
    SOURCE_TYPE = "mongo"

    def _read_dependency(self, dep: TableDependency) -> DataFrame:
        return self._read_collection(self.source_config.database, dep.name)

    def _read_main_table(self) -> DataFrame:
        return self._read_collection(self.source_config.database, self.table_name)

    def _read_collection(self, database: str, table: str) -> DataFrame:
        schema = self.main_table.source_schema
        if not schema:
            raise ValueError(f"Schema not found for table '{table}'")

        reader = (
            self.session.read.format("mongodb")
            .option("database", database)
            .option("collection", table)
            .schema(schema)
        )

        condition = self.conditions.get(table)
        if condition:
            reader = reader.option("aggregation.pipeline", condition)

        return reader.load()