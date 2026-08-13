from pydeequ.checks import Check, CheckLevel

from src.data_quality import BaseTest


class TestStatus(BaseTest):

    stage = "silver"
    table_name = "status"

    def test_completeness(self):
        check = (
            Check(
                self.session,
                CheckLevel.Error,
                "Completeness Check",
            )
            .isComplete(
                "id",
                "ID shouldn't have null value",
            )
            .isComplete(
                "status",
                "STATUS shouldn't have null value",
            )
        )

        self.run_tests(check)

    def test_uniqueness(self):
        check = (
            Check(
                self.session,
                CheckLevel.Error,
                "Uniqueness Check",
            )
            .isUnique(
                "id",
                "ID must be unique",
            )
        )

        self.run_tests(check)

    def test_string(self):
        check = (
            Check(
                self.session,
                CheckLevel.Warning,
                "String Validation",
            )
            .isContainedIn(
                column="status",
                allowed_values=[
                    "paid",
                    "unpaid",
                    "cancelled",
                    "refunded",
                ],
                hint="Invalid status",
            )
        )

        self.run_tests(check)

    def test_dataset(self):
        check = (
            Check(
                self.session,
                CheckLevel.Error,
                "Dataset Validation",
            )
            .hasSize(
                lambda x: x > 0,
                "Dataset must not be empty",
            )
        )

        self.run_tests(check)