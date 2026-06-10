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

        # trim(lower(origin)) as origin,
        # trim(lower(destination)) as destination,
        # coalesce(distance_km, 0) as distance_km,
        # coalesce(duration_minutes, 0) as duration_minutes,

        routes_dataframe = (self.dataframe
                        .withColumn("origin", F.trim(F.lower("origin")))
                        .withColumn("destination", F.trim(F.lower("destination")))
                        .withColumn("distance_km", F.coalesce("distance_km", F.lit(0)))
                        .withColumn("distance_km", F.coalesce("duration_minutes", F.lit(0)))
        ).alias("r")

        # STATIONS
        stations_table = self.lookup_table_name["stations"]

        stations_dataframe = self.session.read.table(stations_table)
        stations_dataframe.cache()
        stations_dataframe.count()

        # Broadcast once
        stations_df = F.broadcast(stations_dataframe)

        stations_df1 = stations_df.alias("s1")
        stations_df2 = stations_df.alias("s2")


        # TRAINS
        trains_table = self.lookup_table_name["trains"]

        trains_dataframe = self.session.read.table(trains_table).alias("tr")
        trains_dataframe.cache()
        trains_dataframe.count()

        df_joined = (
            routes_dataframe
                .join(
                    other=s1,
                    on=s1.code == r.origin, 
                    how="left"
                )
                .join(
                    other=s2,
                    on=s2.code == r.destination,
                    how="left"
                )
                .join(
                    other=tr,
                    on=tr.id == r.id,
                    how="left"
                )
            .select(
                r.id,
                s1.sk_id.alias("sk_org_station_id"),
                s2.sk_id.alias("sk_dest_station_id"),
                tr.sk_id.alias("sk_train_id"),
                r.distance_km,
                r.duration_minutes
            )
        )

        return df_joined
