from pydeequ.checks import Check, CheckLevel

from src.data_quality import BaseTest


class TestRoutes(BaseTest):

    stage = "silver"
    table_name = "routes"

    def test_completeness(self):
        check = (
            Check(self.session, CheckLevel.Error, "Completeness Check")
            .isComplete("sk_id", "SK_ID shouldn't have null value")
            .isComplete("id", "ID shouldn't have null value")
            .isComplete("sk_org_station_id", "Origin station shouldn't have null value")
            .isComplete("sk_dest_station_id", "Destination station shouldn't have null value")
            .isComplete("sk_train_id", "Train shouldn't have null value")
            .isComplete("distance_km", "Distance shouldn't have null value")
            .isComplete("duration_minutes", "Duration shouldn't have null value")
            .isComplete("is_deleted", "IS_DELETED shouldn't have null value")
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
            .isPositive(
                "distance_km",
                "Distance must be greater than 0",
            )
            .isPositive(
                "duration_minutes",
                "Duration must be greater than 0",
            )
            .hasMax(
                "distance_km",
                lambda x: x <= 10000,
                "Distance looks unrealistic",
            )
            .hasMax(
                "duration_minutes",
                lambda x: x <= 1440,
                "Duration looks unrealistic (more than 24 hours)",
            )
        )

        self.run_tests(check)

    def test_business_rules(self):
        check = (
            Check(self.session, CheckLevel.Warning, "Business Rules")
            .satisfies(
                "sk_org_station_id <> sk_dest_station_id",
                "origin_destination_validation",
                lambda x: x == 1.0,
            )
            .satisfies(
                "(distance_km / (duration_minutes / 60.0)) BETWEEN 20 AND 300",
                "average_speed_validation",
                lambda x: x >= 0.98,
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