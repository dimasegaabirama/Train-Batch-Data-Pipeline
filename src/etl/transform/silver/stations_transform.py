import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from src.etl.transform import BaseTransform


class StationsTransform(BaseTransform):

    def transform(self) -> DataFrame:
        """
        Transform the input stations DataFrame by normalizing the 'city' column
        and removing duplicate rows.

        Steps
        -----
        1. Normalize the 'city' column using `normalize_string`:
           - Trim spaces
           - Convert to lowercase
           - Fill nulls with 'unknown'
        2. Drop duplicate rows to ensure unique station entries.

        Parameters
        ----------
        dataframe : DataFrame
            Input Spark DataFrame containing stations data. Expected column:
            ['city', ...].

        Returns
        -------
        DataFrame
            Transformed DataFrame with normalized 'city' column and duplicates removed.

        Notes
        -----
        - Only the 'city' column is normalized; other columns are preserved.
        - This transform is a prerequisite for `RoutesTransform`.
        """

        try:
            self.dataframe = (self.dataframe
                .withColumn("sk_id",  F.abs(F.xxhash64(F.col("id"), F.col("updated_at"))))
                .withColumn("name", F.trim(F.lower("name")))
                .withColumn("city", F.coalesce(F.trim(F.lower("city")), F.lit("unknown")))
                .withColumn("code", F.trim(F.lower("code")))
            )
            return self.dataframe.dropDuplicates("sk_id")
        except Exception as e:
            raise ValueError(f"Error during stations transformation: {e}")
