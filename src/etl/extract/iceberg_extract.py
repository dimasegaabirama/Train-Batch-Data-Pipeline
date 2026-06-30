from .base_extract import BaseExtract
from src.utils.table_utils import create_table_fullname

class IcebergExtract(BaseExtract):
    
    def __init__(self, logger, session, config, table_name, catalog_type, stage, condition, schema = None, **kwargs):
        super().__init__(logger, session, config, table_name, schema, **kwargs)
        self.catalog_type = catalog_type
        self.stage = stage
        self.condition = condition

    def resolve_full_table_name(self) -> str:

        return create_table_fullname(
            catalog_name=self.config.get_catalog_name(),
            schema_name=self.config.get_schema_name(self.stage),
            table_name=self.table_name
        )

    def extract(self):

        try:
            self.logger.debug(
                f"[Extract Iceberg] Reading table {self.table_name} from Iceberg..."
            )

            df = self.session.read

            if self.schema:
                self.logger.debug(f"[Extract Iceberg] Applying schema registry '{self.table_name}'")
                df = df.schema(self.schema)

            df = df.table(self.resolve_full_table_name())

            if self.condition:
                self.logger.debug(f"[Extract Iceberg] Applying condition on '{self.table_name}'")
                df = df.where(self.condition)

            self.logger.debug(
                f"[Extract Iceberg] Successfully read from '{self.table_name}'."
            )

            return df

        except Exception as e:
            self.logger.exception(
                f"[Extract Iceberg] Failed to read '{self.table_name}': {e}"
            )
            raise
