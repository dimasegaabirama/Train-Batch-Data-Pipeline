import pyspark.sql.functions as F

from src.core import DATE_COLUMNS
from src.etl.transform import BaseTransform
from src.models.etl_config import TransformResult


class BronzeTransform(BaseTransform):
    def transform(self) -> TransformResult:
        """
        Transform the input DataFrame by normalizing columns.

        Steps
        -----
        1. Rename '_id' to 'id'.
        2. Convert any column listed in DATE_COLUMNS that is present in the
           DataFrame to a timestamp.

        Returns
        -------
        TransformResult
            The extract result's metadata paired with the transformed DataFrame.
        """
        try:
            transformed_df = self.dataframe.withColumnRenamed("_id", "id")

            for column in DATE_COLUMNS:
                if column in transformed_df.columns:
                    transformed_df = transformed_df.withColumn(
                        column, F.to_timestamp(column)
                    )

            return TransformResult.from_extract(self.extract_result, transformed_df)

        except Exception as e:
            raise RuntimeError(f"Error during bronze transformation: {e}") from e