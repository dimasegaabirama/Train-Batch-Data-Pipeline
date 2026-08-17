import pytest
from pydeequ.checks import Check, CheckLevel

from src.core import DataQualityContext, TableManager
from src.data_quality.base_test import BaseTest


BRONZE_TABLES = TableManager().get_tablenames("bronze")


class TestBronze(BaseTest):
    """Data quality checks that run against every table in the bronze layer."""

    stage = "bronze"

    @pytest.fixture(autouse=True, params=BRONZE_TABLES)
    def setup(self, session, request):
        self.session = session
        self.table_name = request.param

        self.dataframe = getattr(DataQualityContext.get(), "cleaned_dataframe", None)
        if self.dataframe is None:
            raise ValueError(
                "DataQualityContext does not contain 'cleaned_dataframe'. "
                "Ensure it is set before running tests."
            )

    def test_completeness(self):
        check = Check(
            self.session, CheckLevel.Warning, "Bronze Completeness Check"
        ).isComplete("id", "ID Shouldn't have null value !!")

        self.run_tests(check)