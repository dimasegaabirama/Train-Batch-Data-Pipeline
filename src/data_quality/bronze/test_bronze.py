import pytest
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite

from src.core import DATE_COLUMNS
from src.core import TableManager
from src.data_quality.base_test import BaseTest
from pyspark.sql import SparkSession


BRONZE_TABLES = TableManager().get_tablenames("bronze")

class TestBronze(BaseTest):

    stage = "bronze"

    @pytest.fixture(autouse=True, params=BRONZE_TABLES)
    def setup(self, session: SparkSession, request):
        self.session = session
        self.table_name = request.param

        self.table_fullname = self._table_manager.get_table_fullname(
            self.table_name,
            self.stage,
        )

        self.dataframe = session.read.table(self.table_fullname)


    def test_completeness(self):
        check = Check(self.session, CheckLevel.Warning, "Bronze Completeness Check").isComplete(
            "id", "ID Shouldn't have null value !!"
        )

        self.run_tests(check)