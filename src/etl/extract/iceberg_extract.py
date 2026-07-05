from .base_extract import BaseExtract

class IcebergExtract(BaseExtract):

    def __init__(self, logger, session, config, table_name, condition=None, table_schema=None, **extra):
        super().__init__(logger, session, config, table_name, condition, table_schema, **extra)
        self.full_table_name = self.resolve_full_table_name()

    def extract(self):

        try:
            self.logger.debug(
                f"[Extract Iceberg] Reading table {self.full_table_name} from Iceberg..."
            )

            df = self.session.read

            if self.schema:
                self.logger.debug(f"[Extract Iceberg] Applying schema registry '{self.full_table_name}'")
                df = df.schema(self.table_schema)

            df = df.table(self.full_table_name)

            if self.condition:
                self.logger.debug(f"[Extract Iceberg] Applying condition on '{self.full_table_name}'")
                df = df.where(self.condition)

            self.logger.debug(
                f"[Extract Iceberg] Successfully read from '{self.full_table_name}'."
            )

            return df

        except Exception as e:
            self.logger.exception(
                f"[Extract Iceberg] Failed to read '{self.resolve_full_table_name()}': {e}"
            )
            raise
