import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from src.etl.transform import BaseTransform
from src.core import DATE_COLUMNS

class BronzeTransform(BaseTransform):

    def __init__(self, logger, session, config, dataframe, **extra):
        super().__init__(logger, session, config, dataframe, **extra)

    @staticmethod
    def requires():
        return None

    def transform(self) -> DataFrame:
        """
        Transform the input DataFrame by normalizing columns and filtering rows.

        Steps
        -----
        1. Rename '_id' to 'id'.
        2. Convert 'created_at' to timestamp.
        3. Add 'load_at' column with current date.
        4. If 'updated_at' exists, convert to timestamp and filter rows where:
           start_date <= updated_at < end_date

        Parameters
        ----------
        dataframe : DataFrame
            Input Spark DataFrame to transform.

        Returns
        -------
        DataFrame
            Transformed DataFrame with normalized columns and filtered rows.
        """
    
        df = self.dataframe.withColumnRenamed("_id", "id")

        for column in DATE_COLUMNS:
            if column in df.columns:
                df = df.withColumn(
                    column,
                    F.to_timestamp(column)
                )

        return df


if __name__ == "__main__":
    pass
