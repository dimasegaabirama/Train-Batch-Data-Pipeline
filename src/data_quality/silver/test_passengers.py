from pydeequ.checks import Check, CheckLevel

from src.data_quality import BaseTest


class TestPassengers(BaseTest):
    def test_completeness(self):
        check = (
            Check(self.session, CheckLevel.Error, "Completeness Check")
            .isComplete("sk_id", "SK_ID shouldn't have null value")
            .isComplete("id", "ID shouldn't have null value")
            .isComplete("name", "NAME shouldn't have null value")
            .isComplete("start_date", "START_DATE shouldn't have null value")
            .isComplete("is_active", "IS_ACTIVE shouldn't have null value")
            .areAnyComplete(["phone", "email"], "PHONE or EMAIL must not be null")
        )

        self.run_tests(check)

    def test_string(self):
        check = (
            Check(self.session, CheckLevel.Warning, "String Format Check")
            .isContainedIn(
                "gender",
                ["male", "female", "unknown"],
                "Gender must be male/female/unknown",
            )
            .containsEmail("email", "Email format invalid")
            .hasMinLength(
                "phone", lambda x: x >= 10, "Phone must be at least 10 digits"
            )
            .hasMaxLength("phone", lambda x: x <= 12, "Phone must be at most 12 digits")
        )

        self.run_tests(check)

    def test_uniqueness(self):
        check = Check(self.session, CheckLevel.Error, "Uniqueness Check").isUnique(
            "sk_id", "SK_ID must be unique"
        )

        self.run_tests(check)
