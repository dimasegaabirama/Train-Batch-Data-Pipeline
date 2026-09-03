import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import BooleanType, DateType, DecimalType, DoubleType, IntegerType, LongType, StringType, TimestampType

from src.etl.transform import BaseTransform
from src.models.etl_config import TransformResult


class TrainPerformance(BaseTransform):
    def transform(self):
        try:
            tickets_dataframe = self.dependencies["tickets"]
            train_dataframe = self.dependencies["trains"]

            joined_dataframe = tickets_dataframe.alias("f").join(
                train_dataframe.alias("t"),
                on=(
                    (F.col("f.train_sk_id") == F.col("t.sk_id"))
                    & (F.col("f.departure_date") >= F.col("t.start_date"))
                    & (
                        F.col("t.end_date").isNull()
                        | (F.col("f.departure_date") < F.col("t.end_date"))
                    )
                ),
                how="left",
            )

            train_performance_dataframe = joined_dataframe.groupBy(
                F.to_date(F.col("f.departure_date")).alias("departure_date"),
                F.col("f.train_sk_id"),
                F.col("t.name"),
                F.col("t.type"),
                F.col("t.capacity")
            ).agg(
                F.count(
                    F.when(F.col("f.paid_at").isNotNull(), F.col("f.ticket_id"))
                ).alias("total_tickets_sold"),
                F.count(
                    F.when(F.col("f.cancelled_at").isNotNull(), F.col("f.ticket_id"))
                ).alias("total_cancelled_tickets"),
                F.count(
                    F.when(
                        F.col("f.cancelled_at").isNull()
                        & F.col("f.paid_at").isNotNull(),
                        F.col("f.ticket_id"),
                    )
                ).alias("net_tickets_sold"),
                F.sum(
                    F.when(
                        F.col("f.cancelled_at").isNull()
                        & F.col("f.paid_at").isNotNull(),
                        F.col("f.final_price"),
                    ).otherwise(0)
                ).alias("total_revenue"),
                F.sum(F.when(F.col("f.family_flag") == True, 1).otherwise(0)).alias(
                    "family_ticket_count"
                ),
                F.sum(F.when(F.col("f.has_promo") == True, 1).otherwise(0)).alias(
                    "promo_ticket_count"
                ),
                F.max(
                    F.when(
                        F.col("f.cancelled_at") > F.col("f.departure_date"), True
                    ).otherwise(False)
                ).alias("cancelled_after_departure_flag"),
            )

            result_df = (
                train_performance_dataframe.withColumn(
                    "occupancy_rate",
                    F.round((F.col("net_tickets_sold") / F.col("capacity")) * 100, 2),
                )
                .withColumn(
                    "is_fully_booked", F.col("net_tickets_sold") >= F.col("capacity")
                )
                .withColumn("updated_at", F.current_timestamp())
            )

            return self._build_result(
                result_df.select(
                    F.col("departure_date")                 .cast(DateType),
                    F.col("train_sk_id")                    .cast(LongType),
                    F.col("name")                           .cast(StringType),
                    F.col("type")                           .cast(StringType),
                    F.col("capacity")                       .cast(IntegerType),
                    F.col("total_tickets_sold")             .cast(IntegerType),
                    F.col("total_cancelled_tickets")        .cast(IntegerType),
                    F.col("net_tickets_sold")               .cast(IntegerType),
                    F.col("total_revenue")                  .cast(DecimalType(18, 2)),
                    F.col("family_ticket_count")            .cast(IntegerType),
                    F.col("promo_ticket_count")             .cast(IntegerType),
                    F.col("cancelled_after_departure_flag") .cast(BooleanType),
                    F.col("occupancy_rate")                 .cast(DoubleType),
                    F.col("is_fully_booked")                .cast(BooleanType),
                    F.col("updated_at")                     .cast(TimestampType)
                )
            )
        
        except Exception as e:
            raise RuntimeError(
                f"Error during train performance transformation: {e}"
            ) from e
