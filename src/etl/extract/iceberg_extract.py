from .base_extract import BaseExtract

class IcebergExtract(BaseExtract):
    def extract(self):

        try:
            df = self.session.read.table(self.table_fullname)

            if self.condition is not None:
                df = df.where(self.condition)

            return df

        except Exception as e:
            raise RuntimeError(f"Failed to extract data for table '{self.table_fullname}': {e}") from e