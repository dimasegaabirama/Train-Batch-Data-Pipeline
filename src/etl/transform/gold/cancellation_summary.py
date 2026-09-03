import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType, DateType, DoubleType, LongType, IntegerType, TimestampType

from src.etl.transform import BaseTransform


class CancellationSummary(BaseTransform):
    def transform(self):
        try:
            tickets_dataframe = self.dependencies["tickets"]
            tickets_dataframe = tickets_dataframe.withColumn(
                "booking_date", F.to_date("created_at")
            )

            cancellation_summary_dataframe = tickets_dataframe.groupBy(
                "booking_date", "route_sk_id", "class_id"
            ).agg(
                F.count("ticket_id").alias("total_tickets"),
                F.count(F.when(F.col("paid_at").isNotNull(), F.col("ticket_id"))).alias(
                    "total_tickets_paid"
                ),
                F.count(
                    F.when(F.col("cancelled_at").isNotNull(), F.col("ticket_id"))
                ).alias("total_tickets_cancelled"),
                F.count(
                    F.when(F.col("refunded_at").isNotNull(), F.col("ticket_id"))
                ).alias("total_tickets_refunded"),
                F.count(
                    F.when(
                        F.col("paid_at").isNull() & F.col("cancelled_at").isNotNull(),
                        F.col("ticket_id"),
                    )
                ).alias("cancelled_before_payment"),
                F.count(
                    F.when(
                        F.col("paid_at").isNotNull()
                        & F.col("cancelled_at").isNotNull(),
                        F.col("ticket_id"),
                    )
                ).alias("cancelled_after_payment"),
                F.count(
                    F.when(
                        F.col("cancelled_at").isNotNull()
                        & F.col("refunded_at").isNull(),
                        F.col("ticket_id"),
                    )
                ).alias("cancelled_not_yet_refunded"),
                F.sum(
                    F.when(
                        F.col("paid_at").isNotNull()
                        & F.col("cancelled_at").isNotNull(),
                        F.col("final_price"),
                    ).otherwise(0)
                ).cast(DecimalType(18, 2)).alias("total_revenue_lost"),
                F.avg(
                    F.when(
                        F.col("cancelled_at").isNotNull(),
                        (
                            F.col("cancelled_at").cast("long")
                            - F.col("created_at").cast("long")
                        )
                        / 3600,
                    )
                ).alias("avg_hours_to_cancel"),
            )

            result_df = (
                cancellation_summary_dataframe.withColumn(
                    "cancellation_rate",
                    F.round(
                        F.col("total_tickets_cancelled")
                        / F.col("total_tickets"),
                        4,
                    ),
                )
                .withColumn(
                    "cancelled_after_payment_rate",
                    F.round(
                        F.col("cancelled_after_payment") / F.col("total_tickets_paid"),
                        4,
                    ),
                )
                .withColumn("updated_at", F.current_timestamp())
            )

            return self._build_result(
                result_df.select(
                    F.col("booking_date")                   .cast(DateType),
                    F.col("route_sk_id")                    .cast(LongType),
                    F.col("class_id")                       .cast(IntegerType),
                    F.col("total_tickets")                  .cast(IntegerType),
                    F.col("total_tickets_paid")             .cast(IntegerType),
                    F.col("total_tickets_cancelled")        .cast(IntegerType),
                    F.col("total_tickets_refunded")         .cast(IntegerType),
                    F.col("cancelled_before_payment")       .cast(IntegerType),
                    F.col("cancelled_after_payment")        .cast(IntegerType),
                    F.col("cancelled_not_yet_refunded")     .cast(IntegerType),
                    F.col("total_revenue_lost")             .cast(DecimalType(18, 2)),
                    F.col("avg_hours_to_cancel")            .cast(DoubleType),
                    F.col("cancellation_rate")              .cast(DoubleType),
                    F.col("cancelled_after_payment_rate")   .cast(DoubleType),
                    F.col("updated_at")                     .cast(TimestampType)
                )
            )
        
        except Exception as e:
            raise RuntimeError(
                f"Error during cancellation summary transformation: {e}"
            ) from e
