
from pyspark.sql.dataframe import DataFrame
from .base_extract import BaseExtract
from src.core import SourceManager

class MongoExtract(BaseExtract):

    SOURCE_TYPE = "mongo"

    def extract(self):

        database = self.source_config.database
        condition = self.condition

        dataframes = {}

        try:
            for table_name in self.table_names:

                reader = (
                    self.session.read.format("mongodb")
                    .option("database", database)
                    .option("collection", table_name)
                )

                if condition is not None:
                    reader = reader.option("aggregation.pipeline", self.condition)

                schema = self.table_schemas.get(table_name)

                if schema:
                    reader = reader.schema(schema)
                if not schema:
                    raise ValueError(f"Schema not found for table '{table_name}' in stage '{self.upstream_stage}'")

                df = reader.load()

                dataframes[table_name] = df

            return dataframes

        except Exception as e:
            raise RuntimeError(f"Failed to extract data for table '{self.table_name}': {e}") from e


if __name__ == "__main__":
    pass
