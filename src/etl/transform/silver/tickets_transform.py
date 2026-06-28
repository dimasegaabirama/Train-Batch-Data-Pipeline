import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import TimestampType, BooleanType
from pyspark.sql import Window

from src.etl.transform.base_transform import BaseTransform


class TicketsTransform(BaseTransform):
    def __init__(self, session, config, dataframe, lookup_table_name, **kwargs):
        super().__init__(session, config, dataframe, **kwargs)
        self.lookup_table_name = lookup_table_name

    def transform(self) -> DataFrame:
        """
        Clean, deduplicate, and enrich raw tickets into a fact table.

        Steps
        -----
        1. Normalize strings (lower/trim), cast types, fill nulls with defaults.
        2. Compute derived columns: ``final_price``, ``booking_lead_days``,
           ``is_weekend``, ``day_of_week``, ``family_flag``, ``has_child``, ``has_promo``.
        3. Deduplicate per ``ticket_id`` (latest ``created_at``); aggregate
           ``paid_at``, ``cancelled_at``, ``refunded_at`` across all historical rows.
        4. Left-join broadcast lookup tables (routes, class, status, payment)
           and SCD2 dimensions (trains, passengers) validated against ``departure_date``.

        Returns
        -------
        DataFrame
            Fact table with surrogate keys, dimension IDs, timestamps,
            pricing fields, and boolean flags. Nulls in FK columns indicate
            no matching dimension record found.
        """

        tickets_dataframe = (
            self.dataframe
            .withColumn("ticket_id", F.lower(F.trim(F.col("ticket_id"))))
            .withColumn("departure_date", F.col("departure_date").cast(TimestampType()))
            .withColumn("price", F.coalesce("price", F.lit(0.0)))
            .withColumn("discount", F.coalesce("discount", F.lit(0.0)))
            .withColumn("final_price", F.round(F.col("price") * (1 - F.col("discount")), 2))
            .withColumn("class", F.coalesce(F.lower(F.trim(F.col("class"))), F.lit('regular')))
            .withColumn("status", F.lower(F.trim(F.col("status"))))
            .withColumn("payment", F.lower(F.trim(F.col("payment.method"))))
            .withColumn("has_child", F.coalesce(F.col("extra_info.child_discount").cast(BooleanType()), F.lit(False)))
            .withColumn("family_flag", F.when(F.col("extra_info.family_members") > 1, F.lit(True)).otherwise(F.lit(False)))
            .withColumn("has_promo", F.when(F.col("extra_info.promo_code").isNotNull(), F.lit(True)).otherwise(F.lit(False)))
            .withColumn("day_of_week", F.dayofweek(F.col("departure_date")))
            .withColumn("is_weekend", F.dayofweek(F.col("departure_date")).isin([1, 7]))
            .withColumn("booking_lead_days", F.greatest(F.datediff(F.col("departure_date"), F.col("created_at")), F.lit(0)))
        )

        window_latest = Window.partitionBy("ticket_id").orderBy(F.col("created_at").desc())
        window_timestamp = Window.partitionBy("ticket_id")

        tickets_deduped = (
            tickets_dataframe
            .withColumn("rn", F.row_number().over(window_latest))
            .withColumn("active_status",      F.first("status").over(window_latest))
            .withColumn("paid_at",            F.max(F.when(F.col("status") == "paid", F.col("created_at"))).over(window_timestamp))
            .withColumn("cancelled_at",       F.max(F.when(F.col("status") == "cancelled", F.col("created_at"))).over(window_timestamp))
            .withColumn("refunded_at",        F.max(F.when(F.col("status") == "refunded", F.col("created_at"))).over(window_timestamp))
            .filter(F.col("rn") == 1) 
        ).alias("td")

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
                    tickets_deduped
                        .join(
                            routes_df,
                            (F.col("r.id") == F.col("td.route_id")),
                            "left"
                        )
                        .join(
                            trains_df,
                            (F.col("tr.id") == F.col("td.train_id")) &
                            (F.col("td.departure_date") >= F.col("tr.start_date")) &
                            (F.col("tr.end_date").isNull() | (F.col("td.departure_date") < F.col("tr.end_date"))),
                            "left"
                        )
                        .join(
                            passengers_dataframe,
                            (F.col("p.id") == F.col("td.passenger_id")) &
                            (F.col("td.departure_date") >= F.col("p.start_date")) &
                            (F.col("p.end_date").isNull() | (F.col("td.departure_date") < F.col("p.end_date"))),
                            "left"
                        )
                        .join(class_df, F.col("cl.class_name") == F.col("td.class"), "left")
                        .join(status_df, F.col("st.status") == F.col("td.active_status"), "left")
                        .join(payment_df, F.col("py.method") == F.col("td.payment"), "left")
                        .select(
                            F.col("td.ticket_id"),
                            F.col("r.sk_id")                .alias("route_sk_id"),
                            F.col("p.sk_id")                .alias("passenger_sk_id"),
                            F.col("tr.sk_id")               .alias("train_sk_id"),
                            F.col("cl.id")                  .alias("class_id"),
                            F.col("py.id")                  .alias("payment_id"),
                            F.col("st.id")                  .alias("active_status_id"),
                            F.col("td.day_of_week"),         
                            F.col("td.booking_lead_days"),   
                            F.col("td.departure_date"),      
                            F.col("td.paid_at"),             
                            F.col("td.cancelled_at"),
                            F.col("td.refunded_at"),
                            F.col("td.created_at"),
                            F.col("td.price"),               
                            F.col("td.discount"),            
                            F.col("td.final_price"),
                            F.col("td.family_flag"),        
                            F.col("td.has_child"),           
                            F.col("td.has_promo"),           
                            F.col("td.is_weekend")   
                        )
        )
        return tickets_cleaned
