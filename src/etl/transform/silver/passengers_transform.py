import pyspark.sql.functions as F

from src.etl.transform import BaseTransform
from src.models.etl_config import TransformResult


class PassengersTransform(BaseTransform):
    def transform(self) -> TransformResult:
        """
        Build a surrogate key and normalize passenger columns, deduplicating by it.

        Steps
        -----
        1. Add 'sk_id': a hash of 'id' and 'updated_at', used as the surrogate key.
        2. Normalize 'name' and 'email' by trimming whitespace and lowercasing.
        3. Normalize 'gender' the same way, defaulting to 'unknown' when null.
        4. Drop duplicate rows based on 'sk_id'.

        Returns
        -------
        TransformResult
            The extract result's metadata paired with the transformed DataFrame.
        """

        try:
            transformed_df = (
                self.dataframe.withColumn(
                    "sk_id", F.abs(F.xxhash64(F.col("id"), F.col("updated_at")))
                )
                .withColumn("name", F.trim(F.lower("name")))
                .withColumn(
                    "gender", F.coalesce(F.trim(F.lower("gender")), F.lit("unknown"))
                )
                .withColumn("email", F.trim(F.lower("email")))
                .withColumn("phone", F.trim(F.col("phone")))
                .withColumn("is_active", F.lit(True).cast("boolean"))
                .withColumn("start_date", F.to_timestamp("updated_at"))
                .withColumn("end_date", F.lit(None).cast("timestamp"))
                .dropDuplicates(["sk_id"])
            ).select(
                F.col("sk_id"),
                F.col("id"),
                F.col("name"),
                F.col("gender"),
                F.col("phone"),
                F.col("email"),
                F.col("is_active"),
                F.col("start_date"),
                F.col("end_date")
            )

            return TransformResult.from_extract(self.extract_result, transformed_df)

        except Exception as e:
            raise RuntimeError(f"Error during passengers transformation: {e}") from e