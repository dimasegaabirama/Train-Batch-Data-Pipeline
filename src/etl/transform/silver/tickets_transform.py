import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from src.etl.transform.base_transform import BaseTransform
from src.utils.data_cleaning_utils import (
    normalize_numeric,
    normalize_string,
    normalize_timestamp,
)


class TicketsTransform(BaseTransform):
    def __init__(self, session, config, dataframe, **kwargs):
        super().__init__(session, config, dataframe, **kwargs)

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

        cleaned_col = {
            "seat_number": normalize_string(F.col("seat_number")),
            "status": normalize_string(F.col("status")),
            "class": normalize_string(F.col("class"), "regular"),
            "payment.method": normalize_string(F.col("payment.method"), F.lit("cash")),
            "extra_info.source": normalize_string(
                F.col("extra_info.source"), F.lit("direct")
            ),
            "discount": normalize_numeric(F.col("discount"), F.lit(0.0)),
            "price": normalize_numeric(F.col("price"), F.lit(0)),
            "departure_date": normalize_timestamp(
                F.col("departure_date"), F.current_timestamp()
            ),
            "final_price": (F.col("price") * (1 - F.col("discount"))),
        }

        for colName, col in cleaned_col.items():
            dataframe = self.dataframe.withColumn(colName, col)

        return dataframe.dropDuplicates()
