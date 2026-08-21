from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from src.etl.transform import BaseTransform
from src.models.etl_config import TransformResult


class TrainsTransform(BaseTransform):
    """Transform trains data into the Silver layer."""

    def transform(self) -> TransformResult:
        """
        Transform trains data by:

        1. Generating a surrogate key from `id` and `updated_at`.
        2. Normalizing string columns.
        3. Replacing null capacity with 0.
        4. Removing duplicate records based on `sk_id`.

        Returns
        -------
        TransformResult
            Transformed trains DataFrame wrapped in a TransformResult.

        Raises
        ------
        RuntimeError
            If an error occurs during transformation.
        """

        # sk_id BIGINT,
        # id INT,
        # name STRING,
        # type STRING,
        # capacity INT,
        # is_active BOOLEAN,
        # start_date TIMESTAMP,
        # end_date TIMESTAMP

        try:
            transformed_df = (
                self.dataframe
                .withColumn("sk_id",F.abs(F.xxhash64(F.col("id"),F.col("updated_at"))))
                .withColumn("name",F.trim(F.lower(F.col("name"))))
                .withColumn("type", F.coalesce(F.trim(F.lower(F.col("type"))), F.lit("unknown")))
                .withColumn("capacity", F.coalesce(F.col("capacity"), F.lit(0)))
                .withColumn("is_active", F.lit(True).cast("boolean"))
                .withColumn("start_date", F.to_timestamp(F.col("updated_at")))
                .withColumn("end_date", F.lit(None))
                .dropDuplicates(["sk_id"])
            ).select(
                "sk_id",
                "id",
                "name",
                "type",
                "capacity",
                "is_active",
                "start_date",
                "end_date"
            )

            return TransformResult.from_extract(
                self.extract_result,
                transformed_df
            )

        except Exception as e:
            raise RuntimeError(
                f"Error during trains transformation: {e}"
            ) from e