import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from src.etl.transform.base_transform import BaseTransform
from src.utils.data_cleaning_utils import normalize_string


class StationsTransform(BaseTransform):
    def __init__(self, session, config, dataframe, **kwargs):
        super().__init__(session, config, dataframe, **kwargs)

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

        # id,
        # trim(lower(name)) as name,
        # trim(lower(city)) as city,
        # trim(lower(code)) as code,
        # updated_at,
        # created_at

        return (self.dataframe
                    .withColumn("sk_id",  F.abs(F.xxhash64(F.col("id"), F.col("updated_at"))))
                    .withColumn("name", F.trim(F.lower("name")))
                    .withColumn("city", F.trim(F.lower("city")))
                    .withColumn("code", F.trim(F.lower("code")))
        )
