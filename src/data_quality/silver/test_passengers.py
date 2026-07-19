from pydeequ.checks import Check, CheckLevel

from src.data_quality import BaseTest


class TestPassengers(BaseTest):
    
    stage = "silver"
    table_name = "passengers"

    def test_completeness(self):
        check = (
            Check(self.session, CheckLevel.Error, "Completeness Check")
            .isComplete("sk_id", "SK_ID shouldn't have null value")
            .isComplete("id", "ID shouldn't have null value")
            .isComplete("name", "NAME shouldn't have null value")
            .isComplete("start_date", "START_DATE shouldn't have null value")
            .isComplete("is_active", "IS_ACTIVE shouldn't have null value")
            .areAnyComplete(
                ["phone", "email"],
                "Either PHONE or EMAIL must not be null",
            )
        )

        self.run_tests(check)

    def test_string(self):
        check = (
            Check(self.session, CheckLevel.Warning, "String Validation")
            .isContainedIn(
                "gender",
                ["male", "female", "unknown"],
                "Gender must be male/female/unknown",
            )
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
            .satisfies(
                "email IS NULL OR email RLIKE '^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\\.[A-Za-z]{2,}$'",
                "email_validation",
                lambda x: x == 1.0,
            )
            .satisfies(
                "phone IS NULL OR phone RLIKE '^[0-9]{10,12}$'",
                "phone_validation",
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