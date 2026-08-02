import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from src.etl.transform import BaseTransform

class RefundLoss(BaseTransform):
    def transform(self) -> DataFrame:
        try:
            tickets_dataframe = self.session.read.table(self.lookup_tables["tickets"])
            tickets_dataframe = tickets_dataframe.withColumn("refund_date", F.to_date("refunded_at"))

            refund_loss_dataframe = (
                tickets_dataframe
                .filter(F.col("refunded_at").isNotNull())
                .groupBy("refund_date", "route_sk_id", "class_id")
                .agg(
                    F.count("ticket_id").alias("total_tickets_refunded"),
                    F.sum(F.col("final_price")).alias("total_refund_amount"),
                    (F.sum(F.col("final_price")) / F.count("ticket_id")).alias("avg_refund_amount"),
                    F.avg(
                        F.when(
                            F.col("cancelled_at").isNotNull(),
                            (F.col("refunded_at").cast("long") - F.col("cancelled_at").cast("long")) / 86400
                        )
                    ).alias("avg_days_cancel_to_refund"),
                    F.avg(
                        (F.col("refunded_at").cast("long") - F.col("created_at").cast("long")) / 3600
                    ).alias("avg_hours_to_refund"),
                    F.avg(
                        (F.col("refunded_at").cast("long") - F.col("created_at").cast("long")) / 86400
                    ).alias("avg_days_created_to_refund"),
                    F.count(F.when(F.col("has_promo") == True, F.col("ticket_id"))).alias("total_refunded_with_promo"),
                    F.count(F.when(F.col("family_flag") == True, F.col("ticket_id"))).alias("total_refunded_with_family_flag"),
                )
            )

            refund_loss_dataframe = (
                refund_loss_dataframe
                .withColumn("avg_refund_amount", F.round(F.col("avg_refund_amount"), 2))
                .withColumn("avg_days_cancel_to_refund", F.round(F.col("avg_days_cancel_to_refund"), 2))
                .withColumn("avg_hours_to_refund", F.round(F.col("avg_hours_to_refund"), 2))
                .withColumn("avg_days_created_to_refund", F.round(F.col("avg_days_created_to_refund"), 2))
                .withColumn("updated_at", F.current_timestamp())
            )

            return refund_loss_dataframe
        except Exception as e:
            raise RuntimeError(f"Error during refund loss transformation: {e}") from e

