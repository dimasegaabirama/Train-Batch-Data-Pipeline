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
            .withColumn("status", F.lower(F.trim(F.col("status"))))
            .withColumn("payment", F.lower(F.trim(F.col("payment.method"))))
            .withColumn("has_child", F.coalesce(F.col("extra_info.child_discount"), F.lit(False)))
            .withColumn("family_flag", F.when(F.col("extra_info.family_members") > 1, F.lit(True)).otherwise(F.lit(False)))
            .withColumn("has_promo", F.when(F.col("extra_info.promo_code").isNotNull(), F.lit(True)).otherwise(F.lit(False)))
            .withColumn("day_of_week", F.dayofweek(F.col("departure_date")))
            .withColumn("is_weekend", F.dayofweek(F.col("departure_date")).isin([1, 7]))
            .withColumn("booking_lead_days", F.datediff(F.col("created_at"), F.col("departure_date")))
        ).alias("tc")

        tickets_deduped = (
            tickets_dataframe.groupBy("id")
            .agg(
                F.max_by("price", "created_at").alias("price"),
                F.max_by("discount", "created_at").alias("discount"),
                F.max_by("final_price", "created_at").alias("final_price"),
                F.max_by("class", "created_at").alias("class"),
                F.max_by("status", "created_at").alias("current_status"),
                F.max_by("payment", "created_at").alias("payment"),
                F.max_by("has_child", "created_at").alias("has_child"),
                F.max_by("family_flag", "created_at").alias("family_flag"),
                F.max_by("has_promo", "created_at").alias("has_promo"),
                F.max_by("day_of_week", "created_at").alias("day_of_week"),
                F.max_by("is_weekend", "created_at").alias("is_weekend"),
                F.max_by("booking_lead_days", "created_at").alias("booking_lead_days"),
                F.max_by("departure_date", "created_at").alias("departure_date"),
                F.max_by("route_id", "created_at").alias("route_id"),
                F.max_by("train_id", "created_at").alias("train_id"),
                F.max_by("passenger_id", "created_at").alias("passenger_id"),

                F.max("created_at").alias("created_at"),

                F.max(
                    F.when(F.col("status") == "paid",
                        F.col("created_at"))
                ).alias("paid_at"),

                F.max(
                    F.when(F.col("status") == "cancelled",
                        F.col("created_at"))
                ).alias("cancelled_at"),

                F.max(
                    F.when(F.col("status") == "refunded",
                        F.col("created_at"))
                ).alias("refunded_at")
            )
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
                    F.col("tc.id")                  .alias("id"),
                    F.col("r.sk_id")                .alias("route_sk_id"),
                    F.col("p.sk_id")                .alias("passenger_sk_id"),
                    F.col("tr.sk_id")               .alias("train_sk_id"),
                    F.col("tc.price")               .alias("price"),
                    F.col("tc.discount")            .alias("discount"),
                    F.col("tc.final_price")         .alias("final_price"),
                    F.col("cl.id")                  .alias("class_id"),
                    F.col("st.id")                  .alias("status_id"),
                    F.col("py.id")                  .alias("payment_id"),
                    F.col("tc.family_flag")         .alias("family_flag"),
                    F.col("tc.has_child")           .alias("has_child"),
                    F.col("tc.has_promo")           .alias("has_promo"),
                    F.col("tc.departure_date")      .alias("departure_date"),
                    F.col("tc.day_of_week")         .alias("day_of_week"),
                    F.col("tc.is_weekend")          .alias("is_weekend"),
                    F.col("tc.booking_lead_days")  .alias("booking_lead_days"),
                    F.col("tc.created_at")          .alias("created_at")
                )
        )
        
#  |-- id: integer (nullable = true)
#  |-- route_sk_id: long (nullable = true)
#  |-- passenger_sk_id: long (nullable = true)
#  |-- train_sk_id: long (nullable = true)
#  |-- price: double (nullable = false)
#  |-- discount: double (nullable = false)
#  |-- final_price: double (nullable = true)
#  |-- class_id: integer (nullable = true)
#  |-- status_id: integer (nullable = true)
#  |-- payment_id: integer (nullable = true)
#  |-- family_flag: boolean (nullable = false)
#  |-- has_child: boolean (nullable = false)
#  |-- has_promo: boolean (nullable = false)
#  |-- departure_date: timestamp (nullable = true)
#  |-- days_of_week: integer (nullable = true)
#  |-- is_weekend: boolean (nullable = true)
#  |-- created_at: timestamp (nullable = true)

        return tickets_cleaned
