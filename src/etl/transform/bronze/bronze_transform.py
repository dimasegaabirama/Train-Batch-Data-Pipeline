import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from src.etl.transform.base_transform import BaseTransform


class BronzeTransform(BaseTransform):
    def __init__(self, session, config, dataframe, **kwargs):
        super().__init__(session, config, dataframe, **kwargs)

    @staticmethod
    def requires():
        return None

    def transform(self) -> DataFrame:
        """
        Transform the input DataFrame by normalizing columns and filtering rows.

        Steps
        -----
        1. Rename '_id' to 'id'.
        2. Convert 'created_at' to timestamp.
        3. Add 'load_at' column with current date.
        4. If 'updated_at' exists, convert to timestamp and filter rows where:
           start_date <= updated_at < end_date

        Parameters
        ----------
        dataframe : DataFrame
            Input Spark DataFrame to transform.

        Returns
        -------
        DataFrame
            Transformed DataFrame with normalized columns and filtered rows.
        """

        format_timestamp = "yyyy-MM-dd'T'HH:mm:ssX"

        df = (
            self.dataframe.withColumnRenamed("_id", "id")
            .withColumn("id", F.col("id").isNotNull())
        )

        if "created_at" in df.columns:
            df = df.withColumn(
                "created_at", F.to_timestamp("created_at"), format_timestamp
            )
            

        if "updated_at" in df.columns:
            df = df.withColumn(
                "updated_at", F.to_timestamp("updated_at", format_timestamp)
            )

        return df


if __name__ == "__main__":
    pass
