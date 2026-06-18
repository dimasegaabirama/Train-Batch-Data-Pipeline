import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.types import TimestampType

from src.etl.transform.base_transform import BaseTransform
from src.utils.data_cleaning_utils import normalize_string


class TrainsTransform(BaseTransform):
    def __init__(self, session, config, dataframe, **kwargs):
        super().__init__(session, config, dataframe, **kwargs)

    def transform(self) -> DataFrame:
        """
        Transform the input trains DataFrame by normalizing the 'type' column and dropping duplicates.

        Steps
        -----
        1. Normalize string columns:
        - 'type' → fill null or invalid values with "unknown"

        2. Drop duplicate rows.

        Parameters
        ----------
        dataframe : DataFrame
            Input Spark DataFrame containing trains data. Expected columns include:
            ['type', ...]

        Returns
        -------
        DataFrame
            Transformed trains DataFrame with normalized 'type' column and duplicates removed.

        Notes
        -----
        - This transform is independent and has no prerequisites.
        - Only the 'type' column is normalized in this transformation.
        """

    # id,
    # route_id,
    # passenger_id,
    # train_id,
    # price,
    # coalesce(discount, 0.0) as discount,
    # round(price * (1 - coalesce(discount, 0.0)), 2) as final_price,
    # coalesce(class, 'regular') as class,
    # coalesce(status, 'unknown') as status,
    # coalesce(payment.method, 'direct') as method,
    # cast(departure_date as timestamp),
    # coalesce(extra_info.child_discount, false) as has_child,
    # case when
    #     extra_info.family_members > 1 then true
    #     else false
    # end as family_flag,
    # case when
    #     extra_info.promo_code is not null then true
    #     else false
    # end as has_promo,
    # departure_date,
    # dayofweek(departure_date) as day_of_week,
    # case when
    #     dayofweek(departure_date) in (1, 7) then true
    # else false
    # end as is_weekend,
    # created_at

        day_of_week = F.dayofweek(F.col("departure_date"))

        tickets_dataframe = (
            self.dataframe
                .withColumn("departure_date", F.col("departure_date").cast(TimestampType()))
                .withColumn("discount", F.coalesce(F.col("discount"), F.lit(0.0)))
                .withColumn("price", F.coalesce(F.col("price"), F.lit(0.0)))
                .withColumn("final_price", F.round(F.col("price") * (1 - F.coalesce(F.col("discount"), F.lit(0.0))), 2))
                .withColumn("class", F.coalesce(F.lower(F.trim(F.col("class"))), F.lit("regular")))
                .withColumn("status", F.coalesce(F.lower(F.trim(F.col("status"))), F.lit("unknown")))
                .withColumn("payment_method", F.coalesce(F.col("payment.method"), F.lit("direct")))
                .withColumn("has_child", F.coalesce(F.col("extra_info.child_discount"), F.lit(False)))
                .withColumn("family_flag", F.when(F.col("extra_info.family_members") > 1, True).otherwise(False))
                .withColumn("has_promo", F.when(F.col("extra_info.promo_code").isNotNull(), True).otherwise(False))
                .withColumn("days_of_week", day_of_week)
                .withColumn("is_weekend", F.when(day_of_week.isin([1, 7]), True).otherwise(False))
        ).alias("tk")

        routes_dataframe = F.broadcast(self.session.read("nessie.silver.routes")).where(F.col("is_active") == True).alias("r")
        passengers_dataframe = F.broadcast(self.session.read("nessie.silver.passengers")).where(F.col("is_active") == True).alias("p")
        trains_dataframe = F.broadcast(self.session.read("nessie.silver.trains")).where(F.col("is_active") == True).alias("tr")

        return (
            tickets_dataframe
                .join(routes_dataframe, F.col("r.id") == F.col("tk.route_id"), "left")
                .join(passengers_dataframe, F.col("p.id") == F.col("tk.passenger_id"), "left")
                .join(trains_dataframe, F.col("tr.id") == F.col("tk.train_id"), "left")
                .select(
                    "tk.id",
                    "r.sk_route_id",
                    "p.sk_passenger_id",
                    "tr.sk_train_id",
                    "tk.price",
                    "tk.discount",
                    "tk.final_price",
                    "tk.class",
                    "tk.status",
                    "tk.payment_method",
                    "tk.has_child",
                    "tk.family_flag",
                    "tk.has_promo",
                    "tk.departure_date",
                    "tk.days_of_week",
                    "tk.is_weekend"
                )
        )
    


