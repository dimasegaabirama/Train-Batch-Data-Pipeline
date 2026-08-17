from pydeequ.checks import Check, CheckLevel
from src.data_quality.base_test import BaseTest


class TestBronze(BaseTest):
    """Data quality checks that run against every table in the bronze layer."""

    def test_completeness(self):
        check = Check(
            self.session, CheckLevel.Warning, "Bronze Completeness Check"
        ).isComplete("id", "ID Shouldn't have null value !!")

        self.run_tests(check)