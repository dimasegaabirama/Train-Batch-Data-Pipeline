import pyspark.sql.functions as F

from src.etl.transform.base_transform import BaseTransform
from src.utils.data_cleaning_utils import normalize_numeric


class RoutesTransform(BaseTransform):
    def __init__(self, session, config, dataframe, lookup_table_name, **kwargs):
        super().__init__(session, config, dataframe, **kwargs)
        self.table_name = "routes"
        self.lookup_table_name = lookup_table_name

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
                        .withColumn("sk_id",  F.abs(F.xxhash64(F.col("id"), F.col("updated_at"))))
                        .withColumn("origin", F.trim(F.lower("origin")))
                        .withColumn("destination", F.trim(F.lower("destination")))
                        .withColumn("distance_km", F.coalesce("distance_km", F.lit(0)))
                        .withColumn("distance_km", F.coalesce("duration_minutes", F.lit(0)))
        )

        stations_dataframe = self.session.read.table(self.lookup_table_name["stations"])
        trains_dataframe = self.session.read.table(self.lookup_table_name["trains"])

        stations_df = F.broadcast(stations_dataframe)
        trains_df = F.broadcast(trains_dataframe)

        r = routes_dataframe.alias("r")

        s1 = stations_df.withColumnRenamed("sk_id", "sk_org_station_id").alias("s1")
        s2 = stations_df.withColumnRenamed("sk_id", "sk_dest_station_id").alias("s2")
        tr = trains_df.withColumnRenamed("sk_id", "sk_train_id").alias("tr")

        df_joined = (
            r
            .join(s1, (s1.code == r.origin) & (s1.is_active == True), "left")
            .join(s2, (s2.code == r.destination) & (s2.is_active == True), "left")
            .join(tr, (tr.id == r.train_id) & (tr.is_active == True), "left")
            .select(
                r.sk_id,
                r.id,
                s1.sk_org_station_id,
                s2.sk_dest_station_id,
                tr.sk_train_id,
                r.distance_km,
                r.duration_minutes
            )
        )

        return df_joined
