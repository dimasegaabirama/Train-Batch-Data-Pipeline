from pydeequ.checks import Check, CheckLevel

from src.core import TableManager
from src.data_quality.base_test import BaseTest


BRONZE_TABLES = TableManager().get_tablenames("bronze")

class TestBronze(BaseTest):

    stage = "bronze"

    def test_completeness(self):
        check = Check(self.session, CheckLevel.Warning, "Bronze Completeness Check").isComplete(
            "id", "ID Shouldn't have null value !!"
        )

        self.run_tests(check)