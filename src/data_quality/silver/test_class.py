from pydeequ.checks import Check, CheckLevel

from src.data_quality import BaseTest


class TestClass(BaseTest):

    stage = "silver"

    def test_completeness(self):
        check = (
            Check(self.session, CheckLevel.Error, "Class - Completeness Check")
            .isComplete("id", "ID shouldn't have null value")
            .isComplete("class_name", "class_name shouldn't have null value")
        )
        self.run_tests(check)

    def test_uniqueness(self):
        check = (
            Check(self.session, CheckLevel.Error, "Class - Uniqueness Check")
            .isUnique("id", "ID must be unique")
        )
        self.run_tests(check)

    def test_class_name_allowed_values(self):
        check = (
            Check(self.session, CheckLevel.Warning, "Class - String Validation")
            .isContainedIn(
                column="class_name",
                allowed_values=["vip", "family", "regular", "promo"],
                hint="Invalid class_name value",
            )
        )
        self.run_tests(check)

    def test_row_count_not_empty(self):
        check = (
            Check(self.session, CheckLevel.Error, "Class - Dataset Validation")
            .hasSize(lambda x: x > 0, "Dataset must not be empty")
        )
        self.run_tests(check)