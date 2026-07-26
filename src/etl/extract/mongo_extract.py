
from pyspark.sql.dataframe import DataFrame
from .base_extract import BaseExtract
from src.core import SourceManager

class MongoExtract(BaseExtract):

    SOURCE_TYPE = "mongo"

    def extract(self) -> DataFrame:

        collection = self.table_name
        database = self.source_config.database
        condition = self.condition

        try:
            reader = (
                self.session.read.format("mongodb")
                .option("database", database)
                .option("collection", collection)
            )

            if condition:
                reader = reader.option("aggregation.pipeline", condition)

            if self.table_schema:
                reader = reader.schema(self.table_schema)

            df = reader.load()

            return df

        except Exception as e:
            raise


if __name__ == "__main__":
    pass
