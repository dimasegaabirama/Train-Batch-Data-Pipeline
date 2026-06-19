import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

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

        # WITH TICKETS_CLEANED AS (
        # select
        #     id,
        #     route_id,
        #     passenger_id,
        #     train_id,
        #     price,
        #     coalesce(discount, 0.0) as discount,
        #     round(price * (1 - coalesce(discount, 0.0)), 2) as final_price,
        #     coalesce(lower(trim(class)), 'regular') as class,
        #     coalesce(lower(trim(status)), 'unknown') as status,
        #     coalesce(lower(trim(payment.method)), 'direct') as method,
        #     cast(departure_date as timestamp) as departure_date,
        #     coalesce(extra_info.child_discount, false) as has_child,
        #     case when
        #         extra_info.family_members > 1 then true
        #         else false
        #     end as family_flag,
        #     case when
        #         extra_info.promo_code is not null then true
        #         else false
        #     end as has_promo,
        #     dayofweek(cast(departure_date as timestamp)) as day_of_week,
        #     case when
        #         dayofweek(cast(departure_date as timestamp)) in (1, 7) then true
        #     else false
        #     end as is_weekend,
        #     created_at
        # from tickets_view)

        # SELECT 
        #     tc.id                   as id,
        #     r.sk_id                 as route_sk_id,
        #     p.sk_id                 as passenger_sk_id,
        #     tr.sk_id                as train_sk_id,
        #     tc.price                as price,
        #     tc.discount             as discount,
        #     tc.final_price          as final_price,
        #     cl.id                   as class_id,
        #     st.id                   as status_id,
        #     py.id                   as payment_id,
        #     tc.has_child            as has_child,
        #     tc.family_flag          as family_flag,
        #     tc.has_promo            as has_promo,
        #     tc.dep_date             as departure_date,
        #     tc.day_of_week          as day_of_week,
        #     tc.is_weekend           as is_weekend,
        #     tc.created_at           as created_at
        # FROM (
        #     SELECT 
        #         id,
        #         route_id,
        #         passenger_id,
        #         train_id,
        #         price,
        #         discount,
        #         final_price,
        #         class,
        #         status,
        #         method,
        #         has_child,
        #         family_flag,
        #         has_promo,
        #         departure_date  AS dep_date,
        #         day_of_week,
        #         is_weekend,
        #         created_at
        #     FROM TICKETS_CLEANED
        # ) tc
        # LEFT JOIN nessie.silver.routes r 
        #     ON tc.route_id = r.id 
        #     AND r.is_active = true
        # LEFT JOIN nessie.silver.passengers p 
        #     ON tc.passenger_id = p.id 
        #     AND tc.dep_date >= p.start_date
        #     AND (p.end_date IS NULL OR tc.dep_date < p.end_date)
        # LEFT JOIN nessie.silver.trains tr 
        #     ON tc.train_id = tr.id 
        #     AND tc.dep_date >= tr.start_date
        #     AND (tr.end_date IS NULL OR tc.dep_date < tr.end_date)
        # LEFT JOIN nessie.silver.class cl 
        #     ON tc.class = cl.class_name
        # LEFT JOIN nessie.silver.status st 
        #     ON tc.status = st.status
        # LEFT JOIN nessie.silver.payment py 
        #     ON tc.method = py.method

        tickets_dataframe = (
            self.dataframe
            .withColumn("price", F.coalesce("price", F.lit(0.0)))
            .withColumn("discount", F.coalesce("discount", F.lit(0.0)))
            .withColumn("final_price", F.round(F.col("price") * (1 - F.coalesce(F.col("discount"), F.lit(0.0))), 2))
            .withColumn("class", F.coalesce(F.lower(F.trim("class")), F.lit('regular')))
            .withColumn("status", F.coalesce(F.lower(F.trim("status")), F.lit('unknown')))
            .withColumn("payment", F.coalesce(F.lower(F.trim("payment.method")), F.lit('direct')))
            .withColumn("has_child", F.coalesce(F.col("extra_info.child_discount", F.lit(False))))
            .withColumn("family_flag", F.when(F.col("extra_info.family_members") > 1, F.lit(True)).otherwise(F.lit(False)))
            .withColumn("has_promo", F.when(F.col("extra_info.promo_code").isNotNull(), F.lit(True)).otherwise(F.lit(False)))
            .withColumn("days_of_week", F.dayofweek(F.col("departure_date")))
            .withColumn("is_weekend", F.dayofweek(F.col("departure_date")).isin([x for x in range(1, 8)]))
        ).alias("tc")

        routes_dataframe = self.session.read.table(self.lookup_table_name["routes"])
        trains_dataframe = self.session.read.table(self.lookup_table_name["trains"])
        passengers_dataframe = self.session.read.table(self.lookup_table_name["passengers"]).alias("p")

        routes_df = F.broadcast(routes_dataframe).alias("r")
        trains_df = F.broadcast(trains_dataframe).alias("tr")

        tickets_cleaned = (
            tickets_dataframe
                .join(routes_df, F.col("r.id") == F.col("tc.route_id"), "left")
                    .where(F.col("r.is_active") == True)
                .join(trains_df, F.col("tr.id") == F.col("tc.train_id"), "left")
                    .where(
                        (F.col("tc.departure_date") >= F.col("tr.end_date")) and
                        (F.col("tr.end_date").isNull() or F.col("tc.departure_date") < F.col("tc.departure_date"))
                )
                .join(routes_df, F.col("p.id") == F.col("tc.passenger_id"), "left")
        )

        return dataframe.dropDuplicates()
