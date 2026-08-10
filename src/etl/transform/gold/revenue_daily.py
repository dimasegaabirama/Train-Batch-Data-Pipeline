import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from src.models.etl_config import TransformResult
from src.etl.transform import BaseTransform

class RevenueDaily(BaseTransform):
    def transform(self):
        try:
            tickets_dataframe = self.dependencies["tickets"]

            tickets_with_check = (
                tickets_dataframe
                .filter(F.col("paid_at").isNotNull())
                .withColumn("revenue_date", F.to_date("paid_at"))
                .withColumn("calculated_discount", F.round(F.col("price") - F.col("final_price"), 2))
            )

            revenue_daily_dataframe = (
                tickets_with_check
                .groupBy("revenue_date", "route_sk_id", "class_id")
                .agg(
                    F.count("ticket_id").alias("total_tickets"),
                    F.sum("price").alias("gross_revenue"),
                    F.sum("calculated_discount").alias("total_discount_calculated"),
                    F.sum("final_price").alias("net_revenue"),
                    F.sum(
                        F.when(F.col("refunded_at").isNotNull(), F.col("final_price")).otherwise(0)
                    ).alias("refunded_revenue")
                )
                .withColumn(
                    "net_revenue_after_refund",
                    F.col("net_revenue") - F.col("refunded_revenue")
                )
                .withColumn(
                    "avg_ticket_price",
                    F.round(F.col("net_revenue") / F.col("total_tickets"), 2)
                )
                .withColumn("updated_at", F.current_timestamp())
            )
            
            return TransformResult.from_extract(self.extract_result, revenue_daily_dataframe)
        except Exception as e:
            raise RuntimeError(f"Error during revenue daily transformation: {e}") from e
