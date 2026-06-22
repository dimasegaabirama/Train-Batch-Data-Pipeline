import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import TimestampType

from src.etl.transform.base_transform import BaseTransform
from src.utils.data_cleaning_utils import normalize_string


class TrainsTransform(BaseTransform):
    def __init__(self, session, config, dataframe, lookup_table_name,**kwargs):
        super().__init__(session, config, dataframe, **kwargs)
        self.lookup_table_name = lookup_table_name

    def transform(self) -> DataFrame:
        """
        Transform the input trains DataFrame by normalizing the 'type' column and dropping duplicates.

        Steps
        -----
        1. Normalize string columns:
        - 'type' → fill null or invalid values with "unknown"

        2. Drop duplicate rows.

        Parameters
        ----------
        dataframe : DataFrame
            Input Spark DataFrame containing trains data. Expected columns include:
            ['type', ...]

        Returns
        -------
        DataFrame
            Transformed trains DataFrame with normalized 'type' column and duplicates removed.

        Notes
        -----
        - This transform is independent and has no prerequisites.
        - Only the 'type' column is normalized in this transformation.
        """

        return (self.dataframe
            .withColumn("sk_id",  F.abs(F.xxhash64(F.col("id"), F.col("updated_at"))))
            .withColumn("name", F.trim(F.lower("name")))
            .withColumn("type", F.coalesce(F.trim(F.lower("type")), F.lit("unknown")))
            .withColumn("capacity", F.coalesce("capacity", F.lit(0)))
        )
    


