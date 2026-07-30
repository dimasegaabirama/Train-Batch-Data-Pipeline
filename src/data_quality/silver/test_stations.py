from pydeequ.checks import Check, CheckLevel

from src.data_quality import BaseTest


class TestStations(BaseTest):

    stage = "silver"
    table_name = "stations"

    def test_completeness(self):
        check = (
            Check(self.session, CheckLevel.Error, "Completeness Check")
            .isComplete("sk_id", "SK_ID shouldn't have null value")
            .isComplete("id", "ID shouldn't have null value")
            .isComplete("name", "NAME shouldn't have null value")
            .isComplete("city", "CITY shouldn't have null value")
            .isComplete("code", "CODE shouldn't have null value")
            .isComplete("is_deleted", "IS_DELETED shouldn't have null value")
        )

        self.run_tests(check)

    def test_string(self):
        check = (
            Check(self.session, CheckLevel.Warning, "String Validation")

            .hasMinLength(
                "name",
                lambda x: x >= 3,
                "Station name must be at least 3 characters",
            )

            .hasMaxLength(
                "name",
                lambda x: x <= 100,
                "Station name must be at most 100 characters",
            )

            .hasMinLength(
                "city",
                lambda x: x >= 2,
                "City must be at least 2 characters",
            )

            .hasMaxLength(
                "city",
                lambda x: x <= 50,
                "City must be at most 50 characters",
            )

            .hasMinLength(
                "code",
                lambda x: x >= 2,
                "Code must be at least 2 characters",
            )

            .hasMaxLength(
                "code",
                lambda x: x <= 5,
                "Code must be at most 5 characters",
            )

            .satisfies(
                "code RLIKE '^[A-Z0-9]+$'",
                "station_code_validation",
                lambda x: x == 1.0,
            )
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
            .isUnique("sk_id", "SK_ID must be unique")
        )

        self.run_tests(check)