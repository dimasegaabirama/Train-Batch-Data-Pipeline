import pyspark.sql.functions as F

from src.etl.transform import BaseTransform


class RoutesTransform(BaseTransform):
    def __init__(self, logger, session, config, dataframe, **extra):
        super().__init__(logger, session, config, dataframe, **extra)
        self.lookup_table_name = self.extra["lookup_table_name"]

    def transform(self):
        """
        Transform routes DataFrame by normalizing numeric columns, dropping duplicates,
        and enriching with origin and destination station IDs from stations DataFrame.

        Steps
        -----
        1. Normalize numeric columns 'distance_km' and 'duration_minutes' using
           `normalize_numeric` (filling nulls with 0).
        2. Drop duplicate rows in the routes DataFrame.
        3. Cache and trigger action on stations DataFrame for performance.
        4. Broadcast stations DataFrame to optimize join.
        5. Join routes with stations twice to map origin and destination codes to IDs.
        6. Select and rename relevant columns in the final DataFrame.

        Parameters
        ----------
        dataframe : DataFrame
            Spark DataFrame containing routes data. Expected columns:
            ['id', 'origin', 'destination', 'train_id', 'distance_km',
             'duration_minutes', 'created_at', 'updated_at'].
        stations_dataframe : DataFrame
            Spark DataFrame containing stations data. Expected columns:
            ['id', 'code', ...] for mapping to origin and destination.

        Returns
        -------
        DataFrame
            Transformed routes DataFrame with the following columns:
            ['id', 'org_station_id', 'dest_station_id', 'train_id',
             'distance_km', 'duration_minutes', 'created_at', 'updated_at'].

        Notes
        -----
        - Broadcasting stations DataFrame ensures the join is efficient for large routes tables.
        - Numeric columns are normalized with `normalize_numeric` to avoid nulls.
        - Duplicate rows are removed before joining to avoid redundant data.
        """

        routes_dataframe = (self.dataframe
            .withColumn("sk_id", F.abs(F.xxhash64(F.col("id"), F.col("updated_at"))))
            .withColumn("origin", F.trim(F.lower(F.col("origin"))))
            .withColumn("destination", F.trim(F.lower(F.col("destination"))))
            .withColumn("distance_km", F.coalesce(F.col("distance_km"), F.lit(0)))
            .withColumn("duration_minutes", F.coalesce(F.col("duration_minutes"), F.lit(0)))
        )

        stations_dataframe = self.session.read.table(self.lookup_table_name["stations"])
        trains_dataframe = self.session.read.table(self.lookup_table_name["trains"])

        stations_df = F.broadcast(stations_dataframe)
        trains_df = F.broadcast(trains_dataframe)

        r = routes_dataframe.alias("r")

        s1 = stations_df.withColumnRenamed("sk_id", "sk_org_station_id").where(F.col("is_deleted") == False).alias("s1")
        s2 = stations_df.withColumnRenamed("sk_id", "sk_dest_station_id").where(F.col("is_deleted") == False).alias("s2")
        tr = trains_df.withColumnRenamed("sk_id", "sk_train_id").where(F.col("is_active") == True).alias("tr")

        df_joined = (
            r
            .join(s1, F.col("s1.code") == F.col("r.origin"))
            .join(s2, F.col("s2.code") == F.col("r.destination"))
            .join(tr, F.col("tr.id") == F.col("r.train_id"))
            .select(
                F.col("r.sk_id"),
                F.col("r.id"),
                F.col("s1.sk_org_station_id"),
                F.col("s2.sk_dest_station_id"),
                F.col("tr.sk_train_id"),
                F.col("r.distance_km"),
                F.col("r.duration_minutes")
            )
        )

        return df_joined
