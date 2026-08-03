import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from src.core.constant import CHECKPOINT_DIR
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
            self.session.sparkContext.setCheckpointDir(CHECKPOINT_DIR)

            stations_dataframe = (
                self.dataframe
                    .withColumn("sk_id", F.abs(F.xxhash64(F.col("id"), F.col("updated_at"))))
                    .withColumn("id", F.col("id").cast("int"))
                    .withColumn("name", F.trim(F.lower("name")))
                    .withColumn("city", F.coalesce(F.trim(F.lower("city")), F.lit("unknown")))
                    .withColumn("code", F.trim(F.lower("code")))
            )

            # FIX BUG CATALYST-40548: Use localCheckpoint to avoid nested schema pruning issues
            stations_dataframe = stations_dataframe.localCheckpoint(eager=True)

            stations_dataframe = stations_dataframe.select(
                F.col("sk_id"),
                F.col("id"),
                F.col("name"),
                F.col("city"),
                F.col("code"),
            )

            return stations_dataframe.dropDuplicates(["sk_id"])
        except Exception as e:
            raise RuntimeError(f"Error during stations transformation: {e}") from e
