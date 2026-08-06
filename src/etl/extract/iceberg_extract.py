from typing import Dict

from .base_extract import BaseExtract
from pyspark.sql.dataframe import DataFrame

class IcebergExtract(BaseExtract):
    def extract(self):

        condition = self.condition
        dataframes = {}

        try:
            for table_name in self.table_names.keys():
                df = self.session.read.table(self.table_infos[table_name]["fullname"])
                if condition:
                    df = df.where(condition)

                dataframes[table_name] = df

            return dataframes

        except Exception as e:
            raise RuntimeError(f"Failed to extract data for table '{self.table_fullname}': {e}") from e