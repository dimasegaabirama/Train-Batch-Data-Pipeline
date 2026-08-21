import pyspark.sql.functions as F

from src.etl.transform import BaseTransform
from src.models.etl_config import TransformResult


class StationsTransform(BaseTransform):
    def transform(self) -> TransformResult:
        """
        Normalize station columns and deduplicate by surrogate key.

        Steps
        -----
        1. Add 'sk_id': a hash of 'id' and 'updated_at', used as the surrogate key.
        2. Cast 'id' to int, keeping it as the station's business id.
        3. Normalize 'name' and 'code' by trimming whitespace and lowercasing.
        4. Normalize 'city' the same way, defaulting to 'unknown' when null.
        5. Select the final columns and drop duplicate rows by 'sk_id'.

        Returns
        -------
        TransformResult
            The extract result's metadata paired with the transformed DataFrame,
            containing: 'sk_id', 'id', 'name', 'city', 'code', 'is_deleted'.

        Notes
        -----
        - RoutesTransform joins against this transform's output (via
          `self.dependencies["stations"]`), so its output columns should stay
          in sync with what that join expects.
        """

        try:
            stations_dataframe = (
                self.dataframe.withColumn(
                    "sk_id", F.abs(F.xxhash64(F.col("id"), F.col("updated_at")))
                )
                .withColumn("name", F.trim(F.lower("name")))
                .withColumn(
                    "city", F.coalesce(F.trim(F.lower("city")), F.lit("unknown"))
                )
                .withColumn("code", F.trim(F.lower("code")))
                .withColumn("is_deleted", F.lit(False).cast("boolean"))
                .dropDuplicates(["sk_id"])
            ).select(
                F.col("sk_id"),
                F.col("name"),
                F.col("city"),
                F.col("code"),
                F.col("is_deleted")
            )

            return TransformResult.from_extract(self.extract_result, stations_dataframe)

        except Exception as e:
            raise RuntimeError(f"Error during stations transformation: {e}") from e