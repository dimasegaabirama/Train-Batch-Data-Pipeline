import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from src.etl.transform import BaseTransform
from src.models.etl_config import TransformResult

class PassengersTransform(BaseTransform):

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

        try:
            if self.dataframe is None:
                self.logger.warning("No DataFrame provided for transformation.")
                return self.dataframe

            transformed_df = (
                self.dataframe
                .withColumn("sk_id",  F.abs(F.xxhash64(F.col("id"), F.col("updated_at"))))
                .withColumn("name", F.trim(F.lower("name")))
                .withColumn("gender", F.coalesce(F.trim(F.lower("gender")), F.lit("unknown")))
                .withColumn("email", F.trim(F.lower("email")))
            ).dropDuplicates(["sk_id"])

            return TransformResult.from_extract(self.extract_result, transformed_df)
        except Exception as e:
            raise RuntimeError(f"Error during passengers transformation: {e}") from e
