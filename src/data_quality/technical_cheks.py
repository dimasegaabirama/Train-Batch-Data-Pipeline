from __future__ import annotations

import json
from typing import Any

import pyspark.sql.functions as F
from pyspark.sql.session import SparkSession
from pyspark.sql.types import StructType, _parse_datatype_string
from typing_extensions import List

from src.utils.table_utils import create_table_fullname

from .base_checks import BaseChecks


class TechnicalChecks(BaseChecks):

    def __init__(self, session: SparkSession, catalog_name, schema_name, table_name, config):
        super().__init__(session, catalog_name, schema_name, table_name, config)

    # -------------------------------------------------------------------------
    # Checks
    # -------------------------------------------------------------------------

    def check_schema(self, table_name: str) -> None:
        """Validate dataframe schema against the expected schema from config."""

        expected_schema = _parse_datatype_string(self.config.get_schema_table(table_name=table_name))

        expected = {
            f.name.lower(): str(f.dataType)
            for f in expected_schema.fields
        }

        actual = {
            f.name.lower(): str(f.dataType)
            for f in self.dataframe.schema.fields
        }    

        if actual != expected:
            raise ValueError(f"Schema mismatch!\nExpected: {expected}\nActual: {actual}")

    def check_not_null(self, subset: List[str] = []) -> None:
        """Check that no null values exist outside of the excluded subset."""
        columns_to_check = [c for c in self.dataframe.columns if c not in subset]
        condition = F.lit(False)
        for col in columns_to_check:
            condition = condition | F.col(col).isNull()

        total_null = self.dataframe.filter(condition).count()
        if total_null > 0:
            raise ValueError(f"Found {total_null} row(s) with null values!")

    def check_duplicates(self, subset: List[str] = []) -> None:
        """Check that no duplicate rows exist based on columns outside the subset."""
        columns_to_check = [c for c in self.dataframe.columns if c not in subset]
        total_duplicate = (
            self.dataframe
            .groupBy(columns_to_check)
            .count()
            .filter(F.col("count") > 1)
            .count()
        )
        if total_duplicate > 0:
            raise ValueError(f"Found {total_duplicate} duplicate row(s)!")

    def check_row_count(self, min_count: int = 1, max_count: int = None) -> None:
        """Check that total row count is within the expected min/max bounds."""
        total = self.dataframe.count()
        if total < min_count:
            raise ValueError(f"Row count {total} is below minimum {min_count}!")
        if max_count and total > max_count:
            raise ValueError(f"Row count {total} exceeds maximum {max_count}!")

    def check_accepted_values(self, column: str, accepted_values: List[Any]) -> None:
        """Check that all values in a column belong to the accepted values list."""
        invalid_count = self.dataframe.filter(~F.col(column).isin(accepted_values)).count()
        if invalid_count > 0:
            raise ValueError(
                f"Column '{column}' has {invalid_count} row(s) with values outside {accepted_values}!"
            )

    def check_value_range(self, column: str, min_val=None, max_val=None) -> None:
        """Check that all values in a column fall within the specified range."""
        condition = F.lit(False)
        if min_val is not None:
            condition = condition | (F.col(column) < min_val)
        if max_val is not None:
            condition = condition | (F.col(column) > max_val)

        out_of_range = self.dataframe.filter(condition).count()
        if out_of_range > 0:
            raise ValueError(
                f"Column '{column}' has {out_of_range} row(s) outside range [{min_val}, {max_val}]!"
            )

    def check_regex_contains(self, column: str, pattern: str) -> None:
        """Check that all values in a column match the given regex pattern."""
        invalid_count = (
            self.dataframe
            .filter(F.col(column).isNull() | ~F.col(column).rlike(pattern))
            .count()
        )
        if invalid_count > 0:
            raise ValueError(f"Column '{column}' has {invalid_count} invalid row(s)")

    def check_referential_integrity(
        self,
        column: str,
        ref_column: str,
        ref_catalog_name: str,
        ref_schema_name: str,
        ref_table_name: str,
    ) -> None:
        """Check that all values in a column exist in the referenced table."""
        ref_dataframe = self.session.read.table(
            create_table_fullname(catalog_name=ref_catalog_name, 
                                  schema_name=ref_schema_name, 
                                  table_name=ref_table_name)
        )
        invalid_count = (
            self.dataframe
            .join(ref_dataframe, self.dataframe[column] == ref_dataframe[ref_column], "left_anti")
            .count()
        )
        if invalid_count > 0:
            raise ValueError(
                f"Column '{column}' has {invalid_count} row(s) not found in reference!"
            )

if __name__ == "__main__":
    pass