import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from src.etl.transform.base_transform import BaseTransform
from src.utils.data_cleaning_utils import normalize_string


class PassengersTransform(BaseTransform):
    def __init__(self, session, config, dataframe, **kwargs):
        super().__init__(session, config, dataframe, **kwargs)

    def transform(self) -> DataFrame:
        """
        Normalize the 'city' column and remove duplicate rows.

        Parameters
        ----------
        dataframe : DataFrame
            Input DataFrame with a 'city' column.

        Returns
        -------
        DataFrame
            Transformed DataFrame with normalized 'city' and no duplicates.
        """

        # sk_id BIGINT,
        # id INT,
        # name STRING,
        # gender STRING,
        # phone STRING,
        # email STRING,
        # is_active BOOLEAN,
        # start_date TIMESTAMP,
        # end_date TIMESTAMP

        return (self.dataframe
                    .withColumn("sk_id",  F.abs(F.xxhash64(F.col("id"), F.col("updated_at"))))
                    .withColumn("name", F.trim(F.lower("name")))
                    .withColumn("gender", F.coalesce(F.trim(F.lower("gender")), F.lit("unknown")))
                    .withColumn("email", F.trim(F.lower("name")))
        )
