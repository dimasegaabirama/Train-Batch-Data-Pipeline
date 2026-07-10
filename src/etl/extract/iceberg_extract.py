from .base_extract import BaseExtract

class IcebergExtract(BaseExtract):
    def extract(self):

        try:
            self.logger.debug(
                f"[Extract Iceberg] Reading table {self.table_fullname} from Iceberg..."
            )

            df = self.session.read

            if self.schema:
                self.logger.debug(f"[Extract Iceberg] Applying schema registry '{self.table_fullname}'")
                df = df.schema(self.table_schema)

            df = df.table(self.table_fullname)

            if self.condition:
                self.logger.debug(f"[Extract Iceberg] Applying condition on '{self.table_fullname}'")
                df = df.where(self.condition)

            self.logger.debug(
                f"[Extract Iceberg] Successfully read from '{self.table_fullname}'."
            )

            return df

        except Exception as e:
            self.logger.exception(
                f"[Extract Iceberg] Failed to read '{self.table_fullname}': {e}"
            )
            raise
