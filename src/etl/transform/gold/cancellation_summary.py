import pyspark.sql.functions as F
from pyspark.sql.types import DecimalType

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
                        / F.col("total_tickets_created"),
                        4,
                    ),
                )
                .withColumn(
                    "cancelled_after_payment_rate",
                    F.round(
                        F.col("cancelled_after_payment") / F.col("total_tickets_paid"),
                        4,
                    ).cast(DecimalType(5, 2)),
                )
                .withColumn("updated_at", F.current_timestamp())
            )

            return self._build_result(result_df)
        except Exception as e:
            raise RuntimeError(
                f"Error during cancellation summary transformation: {e}"
            ) from e



# booking_date TIMESTAMP,
# route_sk_id BIGINT,
# class_id INT

# total_tickets INT,
# total_tickets_paid BIGINT,
# total_tickets_cancelled BIGINT,
# total_tickets_refunded BIGINT
# cancelled_before_payment BIGINT,
# cancelled_after_payment BIGINT,
# cancelled_not_yet_refunded BIGINT
# total_revenue_lost DECIMAL(18, 2),
# avg_hours_to_cancel DOUBLE
# cancellation_rate DOUBLE,
# cancelled_after_payment_rate DOUBLE
# updated_at TIMESTAMP


# Expected schema: {
#     'booking_date': 'DateType',
#     'route_sk_id': 'LongType',
#     'class_id': 'IntegerType',

#     'total_tickets': 'LongType',
#     'total_tickets_paid': 'LongType',
#     'total_tickets_cancelled': 'LongType',
#     'total_tickets_refunded': 'LongType',
#     'cancelled_before_payment': 'LongType',
#     'cancelled_after_payment': 'LongType',
#     'cancelled_not_yet_refunded': 'LongType',
#     'total_revenue_lost': 'DecimalType(18,2)',
#     'avg_hours_to_cancel': DOUBLE,
#     'cancellation_rate': DOUBLE,
#     'cancelled_after_payment_rate': DOUBLE,
#     'updated_at': 'TimestampType'
# }

# Actual schema: {
#     'booking_date': 'DateType',
#     'route_sk_id': 'LongType',
#     'class_id': 'IntegerType',
#     'total_tickets_created': 'LongType',
#     'total_tickets_paid': 'LongType',
#     'total_tickets_cancelled': 'LongType',
#     'total_tickets_refunded': 'LongType',
#     'cancelled_before_payment': 'LongType',
#     'cancelled_after_payment': 'LongType',
#     'cancelled_not_yet_refunded': 'LongType',
#     'total_revenue_lost': 'DecimalType(28,2)',
#     'avg_hours_to_cancel': 'DoubleType',
#     'cancellation_rate': 'DoubleType',
#     'cancelled_after_payment_rate': 'DoubleType',
#     'updated_at': 'TimestampType'
#     }
