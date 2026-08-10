import pyspark.sql.functions as F
from pyspark.sql.dataframe import DataFrame

from src.etl.transform import BaseTransform
from src.core import DATE_COLUMNS
from src.models.etl_config import TransformResult

class BronzeTransform(BaseTransform):

    def transform(self) -> DataFrame:
        """
        Transform the input DataFrame by normalizing columns and filtering rows.

        Steps
        -----
        1. Rename '_id' to 'id'.
        2. Convert 'created_at' to timestamp.
        3. If 'updated_at' exists, convert to timestamp and filter rows where:
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

        try:
            if self.dataframe is None:
                self.logger.warning("No DataFrame provided for transformation.")
                return None
            
            transformed_df = self.dataframe.withColumnRenamed("_id", "id")
            for column in DATE_COLUMNS:
                if column in transformed_df.columns:
                    transformed_df = transformed_df.withColumn(
                        column,
                        F.to_timestamp(column)
                    )
            return TransformResult.from_extract(self.extract_result, transformed_df)
        except Exception as e:
            raise RuntimeError(f"Error during bronze transformation: {e}") from e


if __name__ == "__main__":
    pass
