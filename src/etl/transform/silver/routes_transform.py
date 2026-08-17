import pyspark.sql.functions as F

from src.etl.transform import BaseTransform
from src.models.etl_config import TransformResult


class RoutesTransform(BaseTransform):
    def transform(self) -> TransformResult:
        """
        Normalize routes and enrich them with station and train surrogate keys.

        Steps
        -----
        1. Add 'sk_id': a hash of 'id' and 'updated_at', used as the surrogate key.
        2. Normalize 'origin' and 'destination' by trimming whitespace and
           lowercasing, so they match station codes for the join.
        3. Coalesce 'distance_km' and 'duration_minutes' nulls to 0.
        4. Broadcast the 'stations' and 'trains' dependency DataFrames to keep
           the joins efficient.
        5. Filter stations to non-deleted rows and trains to active rows, and
           rename each 'sk_id' so origin, destination, and train keys don't clash.
        6. Join routes to origin stations (by code == origin), destination
           stations (by code == destination), and trains (by id == train_id).
        7. Select the final columns and drop duplicate rows by 'sk_id'.

        Returns
        -------
        TransformResult
            The extract result's metadata paired with the transformed DataFrame,
            containing: 'sk_id', 'id', 'sk_org_station_id', 'sk_dest_station_id',
            'sk_train_id', 'distance_km', 'duration_minutes'.
        """
        try:
            routes_dataframe = (
                self.dataframe.withColumn(
                    "sk_id", F.abs(F.xxhash64(F.col("id"), F.col("updated_at")))
                )
                .withColumn("origin", F.trim(F.lower(F.col("origin"))))
                .withColumn("destination", F.trim(F.lower(F.col("destination"))))
                .withColumn("distance_km", F.coalesce(F.col("distance_km"), F.lit(0)))
                .withColumn(
                    "duration_minutes", F.coalesce(F.col("duration_minutes"), F.lit(0))
                )
            )

            stations_df = F.broadcast(self.dependencies["stations"])
            trains_df = F.broadcast(self.dependencies["trains"])

            r = routes_dataframe.alias("r")

            s1 = (
                stations_df.withColumnRenamed("sk_id", "sk_org_station_id")
                .where(~F.col("is_deleted"))
                .alias("s1")
            )
            s2 = (
                stations_df.withColumnRenamed("sk_id", "sk_dest_station_id")
                .where(~F.col("is_deleted"))
                .alias("s2")
            )
            tr = (
                trains_df.withColumnRenamed("sk_id", "sk_train_id")
                .where(F.col("is_active"))
                .alias("tr")
            )

            df_joined = (
                r.join(s1, F.col("s1.code") == F.col("r.origin"))
                .join(s2, F.col("s2.code") == F.col("r.destination"))
                .join(tr, F.col("tr.id") == F.col("r.train_id"))
                .select(
                    F.col("r.sk_id"),
                    F.col("r.id"),
                    F.col("s1.sk_org_station_id"),
                    F.col("s2.sk_dest_station_id"),
                    F.col("tr.sk_train_id"),
                    F.col("r.distance_km"),
                    F.col("r.duration_minutes"),
                )
                .dropDuplicates(["sk_id"])
            )

            return TransformResult.from_extract(self.extract_result, df_joined)

        except Exception as e:
            raise RuntimeError(f"Error during routes transformation: {e}") from e