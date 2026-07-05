from pyspark.sql.dataframe import DataFrame
from .base_extract import BaseExtract

SOURCE_TYPE = "mongo"

class MongoExtract(BaseExtract):

    def __init__(self, stage, logger, session, config, table_name, condition=None, table_schema=None, **extra):
        super().__init__(stage, logger, session, config, table_name, condition, table_schema, **extra)

    def get_mongo_database_name(self) -> str:
        cfg = getattr(self.config.get_source_config(SOURCE_TYPE), "database", None)

        if cfg is None:
            raise ValueError(f"Database name from '{SOURCE_TYPE}' not found")

        return cfg

    def extract(self) -> DataFrame:

        collection = self.table_name
        database = self.get_mongo_database_name()

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
