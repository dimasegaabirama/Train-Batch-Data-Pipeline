from pyspark.sql.dataframe import DataFrame

from .base_extract import BaseExtract

from src.utils.table_utils import create_table_fullname


class MongoExtract(BaseExtract):
    def __init__(self, logger, session, config, table_name, condition, schema = None, **kwargs):
        super().__init__(logger, session, config, table_name, schema, **kwargs)
        self.source_name = "mongo"
        self.condition = condition

    def get_database_name(self) -> str:
        cfg = getattr(self.config.get_source_config(self.source_name), "database", None)

        if cfg is None:
            raise ValueError(f"Database name from '{self.source_name}' not found")

        return cfg

    def extract(self) -> DataFrame:

        # Get Source Collection & Database
        collection = self.table_name
        database = self.get_database_name()

        # Get Condition
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

            if self.schema:
                self.logger.debug("[Extract Mongo] Applying schema registry")
                reader = reader.schema(self.schema)

            df = reader.load()

            self.logger.debug(f"[Extract Mongo] Successfully read '{collection}'")

            return df

        except Exception as e:
            self.logger.exception(f"[Extract Mongo] Failed to read '{collection}': {e}")
            raise


if __name__ == "__main__":
    pass
