from pydeequ.checks import Check, CheckLevel

from src.data_quality import BaseTest


class TestTrains(BaseTest):

    stage = "silver"
    table_name = "trains"

    def test_completeness(self):
        check = (
            Check(self.session, CheckLevel.Error, "Completeness Check")
            .isComplete("sk_id", "SK_ID shouldn't have null value")
            .isComplete("id", "ID shouldn't have null value")
            .isComplete("name", "NAME shouldn't have null value")
            .isComplete("type", "TYPE shouldn't have null value")
            .isComplete("capacity", "CAPACITY shouldn't have null value")
            .isComplete("is_active", "IS_ACTIVE shouldn't have null value")
            .isComplete("start_date", "START_DATE shouldn't have null value")
        )

        self.run_tests(check)

    def test_numeric(self):
        check = (
            Check(self.session, CheckLevel.Error, "Numeric Validation")
            .hasMin(
                "id",
                lambda x: x > 0,
                "ID must be greater than 0",
            )
            .hasMin(
                "capacity",
                lambda x: x > 0,
                "Capacity must be greater than 0",
            )
            .hasMax(
                "capacity",
                lambda x: x <= 2000,
                "Capacity looks unrealistic",
            )
        )

        self.run_tests(check)

    def test_string(self):
        check = (
            Check(self.session, CheckLevel.Warning, "String Validation")
            .hasMinLength(
                "name",
                lambda x: x >= 3,
                "Name must be at least 3 characters",
            )
            .hasMaxLength(
                "name",
                lambda x: x <= 100,
                "Name must be at most 100 characters",
            )
            .isContainedIn(
                column = "type",
                allowed_values = [
                    "economy",
                    "business",
                    "executive",
                    "unknown"
                ],
                hint = "Invalid train type"
            )
        )

        self.run_tests(check)

    def test_date(self):
        check = (
            Check(self.session, CheckLevel.Error, "Date Validation")
            .satisfies(
                "end_date IS NULL OR end_date >= start_date",
                "date_validation",
                lambda x: x == 1.0,
            )
        )

        self.run_tests(check)

    def test_dataset(self):
        check = (
            Check(self.session, CheckLevel.Error, "Dataset Validation")
            .hasSize(
                lambda x: x > 0,
                "Dataset must not be empty",
            )
        )

        self.run_tests(check)

    def test_uniqueness(self):
        check = (
            Check(self.session, CheckLevel.Error, "Uniqueness Check")
            .isUnique(
                "sk_id",
                "SK_ID must be unique",
            )
        )

        self.run_tests(check)