import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from src.etl.transform import BaseTransform

class TrainPerformance(BaseTransform):
    def transform(self) -> DataFrame:
        try:
            tickets_dataframe = self.session.read.table(self.lookup_tables["tickets"])
            train_dataframe = self.session.read.table(self.lookup_tables["trains"])

            joined_dataframe = tickets_dataframe.alias("f").join(
                train_dataframe.alias("t"),
                on=(
                    (F.col("f.train_sk_id") == F.col("t.sk_id")) &
                    (F.col("f.departure_date") >= F.col("t.start_date")) &
                    (F.col("t.end_date").isNull() | (F.col("f.departure_date") < F.col("t.end_date")))
                ),
                how="left"
            )

            train_performance_dataframe = joined_dataframe.groupBy(
                "f.departure_date", "f.train_sk_id", "t.name", "t.type", "t.capacity"
            ).agg(
                F.count(F.when(F.col("f.paid_at").isNotNull(), F.col("f.ticket_id"))).alias("total_tickets_sold"),
                F.count(F.when(F.col("f.cancelled_at").isNotNull(), F.col("f.ticket_id"))).alias("total_cancelled_tickets"),
                F.count(F.when(F.col("f.cancelled_at").isNull() & F.col("f.paid_at").isNotNull(), F.col("f.ticket_id"))).alias("net_tickets_sold"),
                F.sum(F.when(F.col("f.cancelled_at").isNull() & F.col("f.paid_at").isNotNull(), F.col("f.final_price")).otherwise(0)).alias("total_revenue"),
                F.sum(F.when(F.col("f.family_flag") == True, 1).otherwise(0)).alias("family_ticket_count"),
                F.sum(F.when(F.col("f.has_promo") == True, 1).otherwise(0)).alias("promo_ticket_count"),
                F.max(F.when(F.col("f.cancelled_at") > F.col("f.departure_date"), True).otherwise(False)).alias("cancelled_after_departure_flag"),
            )

            train_performance_dataframe = (
                train_performance_dataframe
                .withColumn("occupancy_rate", F.round((F.col("net_tickets_sold") / F.col("capacity")) * 100, 2))
                .withColumn("is_fully_booked", F.col("net_tickets_sold") >= F.col("capacity"))
                .withColumn("updated_at", F.current_timestamp())
            )
        except Exception as e:
            raise RuntimeError(f"Error during train performance transformation: {e}") from e