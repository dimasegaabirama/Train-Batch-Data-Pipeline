import pytest
from pyspark.sql import SparkSession
from pydeequ.checks import Check, CheckLevel

from src.core import DataQualityContext
from src.data_quality.base_test import BaseTest


SCOPE_BRONZE = "function"

class TestBronze(BaseTest):
    """Data quality checks that run against every table in the bronze layer."""

    @pytest.fixture(scope=SCOPE_BRONZE, autouse=True)
    def setup(self):
        self.session = DataQualityContext.get_session()

        context = DataQualityContext.get_transform_result()

        self.table_name = context.name
        self.dataframe = context.cleaned_dataframe
        self.target_schema = context.target_schema

    def test_completeness(self):
        check = Check(
            self.session, CheckLevel.Warning, "Bronze Completeness Check"
        ).isComplete("id", "ID Shouldn't have null value !!")

        self.run_tests(check)