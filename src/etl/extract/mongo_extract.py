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
            self.logger.debug(
                f"[Extract Mongo] Reading collection '{collection}' from MongoDB"
            )

            reader = (
                self.session.read.format("mongodb")
                .option("database", database)
                .option("collection", collection)
            )

            if condition:
                self.logger.debug(f"[Extract Mongo] Applying pipeline on '{collection}'")
                reader = reader.option("pipeline", condition)

            if self.table_schema:
                self.logger.debug("[Extract Mongo] Applying schema registry")
                reader = reader.schema(self.table_schema)

            df = reader.load()

            self.logger.debug(f"[Extract Mongo] Successfully read '{collection}'")

            return df

        except Exception as e:
            self.logger.exception(f"[Extract Mongo] Failed to read '{collection}': {e}")
            raise


if __name__ == "__main__":
    pass
