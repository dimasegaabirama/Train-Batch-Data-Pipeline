import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import TimestampType

from src.etl.transform.base_transform import BaseTransform
from src.utils.data_cleaning_utils import (
    normalize_numeric,
    normalize_string,
    normalize_timestamp,
)


class TicketsTransform(BaseTransform):
    def __init__(self, session, config, dataframe, lookup_table_name, **kwargs):
        super().__init__(session, config, dataframe, **kwargs)
        self.lookup_table_name = lookup_table_name

    def transform(self) -> DataFrame:
        """
        Transform the input tickets DataFrame by normalizing string, numeric, and timestamp columns,
        computing derived columns, and dropping duplicates.

        Steps
        -----
        1. Normalize string columns:
           - 'seat_number', 'status' → fill null with "unknown"
           - 'class' → fill null with "regular"
           - 'payment.method' → fill null with "cash"
           - 'extra_info.source' → fill null with "direct"
        2. Normalize numeric columns:
           - 'discount' → fill null with 0.0
           - 'price' → fill null with 0
        3. Normalize timestamp column:
           - 'departure_date' → fill null with current timestamp
        4. Compute derived column:
           - 'final_price' = price * (1 - discount)
        5. Drop duplicate rows.

        Parameters
        ----------
        dataframe : DataFrame
            Input Spark DataFrame containing tickets data. Expected columns include:
            ['seat_number', 'status', 'class', 'payment.method', 'extra_info.source',
             'discount', 'price', 'departure_date', ...]

        Returns
        -------
        DataFrame
            Transformed tickets DataFrame with normalized, derived columns and duplicates removed.

        Notes
        -----
        - This transform is independent and has no prerequisites.
        - Derived columns are calculated after normalization to ensure no nulls propagate.
        """

        tickets_dataframe = (
            self.dataframe
            .withColumn("departure_date", F.col("departure_date").cast(TimestampType()))
            .withColumn("price", F.coalesce("price", F.lit(0.0)))
            .withColumn("discount", F.coalesce("discount", F.lit(0.0)))
            .withColumn("final_price", F.round(F.col("price") * (1 - F.col("discount")), 2))
            .withColumn("class", F.coalesce(F.lower(F.trim(F.col("class"))), F.lit('regular')))
            .withColumn("status", F.coalesce(F.lower(F.trim(F.col("status"))), F.lit('unknown')))
            .withColumn("payment", F.coalesce(F.lower(F.trim(F.col("payment.method"))), F.lit('direct')))
            .withColumn("has_child", F.coalesce(F.col("extra_info.child_discount"), F.lit(False)))
            .withColumn("family_flag", F.when(F.col("extra_info.family_members") > 1, F.lit(True)).otherwise(F.lit(False)))
            .withColumn("has_promo", F.when(F.col("extra_info.promo_code").isNotNull(), F.lit(True)).otherwise(F.lit(False)))
            .withColumn("days_of_week", F.dayofweek(F.col("departure_date")))
            .withColumn("is_weekend", F.dayofweek(F.col("departure_date")).isin([1, 7]))
        ).alias("tc")

        routes_dataframe = self.session.read.table(self.lookup_table_name["routes"])
        trains_dataframe = self.session.read.table(self.lookup_table_name["trains"])
        passengers_dataframe = self.session.read.table(self.lookup_table_name["passengers"]).alias("p")
        class_dataframe = self.session.read.table(self.lookup_table_name["class"])
        status_dataframe = self.session.read.table(self.lookup_table_name["status"])
        payment_dataframe = self.session.read.table(self.lookup_table_name["payment"])

        routes_df = F.broadcast(routes_dataframe).alias("r")
        trains_df = F.broadcast(trains_dataframe).alias("tr")
        class_df = F.broadcast(class_dataframe).alias("cl")
        status_df = F.broadcast(status_dataframe).alias("st")
        payment_df = F.broadcast(payment_dataframe).alias("py")

        tickets_cleaned = (
            tickets_dataframe
                .join(
                    routes_df,
                    (F.col("r.id") == F.col("tc.route_id")),
                    "left"
                )
                .join(
                    trains_df,
                    (F.col("tr.id") == F.col("tc.train_id")) &
                    (F.col("tc.departure_date") >= F.col("tr.start_date")) &
                    (F.col("tr.end_date").isNull() | (F.col("tc.departure_date") < F.col("tr.end_date"))),
                    "left"
                )
                .join(
                    passengers_dataframe,
                    (F.col("p.id") == F.col("tc.passenger_id")) &
                    (F.col("tc.departure_date") >= F.col("p.start_date")) &
                    (F.col("p.end_date").isNull() | (F.col("tc.departure_date") < F.col("p.end_date"))),
                    "left"
                )
                .join(class_df, F.col("cl.class_name") == F.col("tc.class"), "left")
                .join(status_df, F.col("st.status") == F.col("tc.status"), "left")
                .join(payment_df, F.col("py.method") == F.col("tc.payment"), "left")
                .select(
                    F.col("tc.id")              .alias("id"),
                    F.col("r.sk_id")            .alias("route_sk_id"),
                    F.col("p.sk_id")            .alias("passenger_sk_id"),
                    F.col("tr.sk_id")           .alias("train_sk_id"),
                    F.col("tc.price")           .alias("price"),
                    F.col("tc.discount")        .alias("discount"),
                    F.col("tc.final_price")     .alias("final_price"),
                    F.col("cl.id")              .alias("class_id"),
                    F.col("st.id")              .alias("status_id"),
                    F.col("py.id")              .alias("payment_id"),
                    F.col("tc.family_flag")     .alias("family_flag"),
                    F.col("tc.has_child")       .alias("has_child"),
                    F.col("tc.has_promo")       .alias("has_promo"),
                    F.col("tc.departure_date")  .alias("departure_date"),
                    F.col("tc.day_of_week")     .alias("day_of_week"),
                    F.col("tc.is_weekend")      .alias("is_weekend"),
                    F.col("tc.created_at")      .alias("created_at")  
                )
        )
        return tickets_cleaned
