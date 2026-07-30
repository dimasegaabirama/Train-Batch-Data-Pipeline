from pydeequ.checks import Check, CheckLevel

from src.data_quality import BaseTest


class TestTickets(BaseTest):

    stage = "silver"
    table_name = "tickets"

    def test_completeness(self):
        check = (
            Check(self.session, CheckLevel.Error, "Completeness Check")
            .isComplete("ticket_id", "TICKET_ID shouldn't have null value")
            .isComplete("route_sk_id", "ROUTE_SK_ID shouldn't have null value")
            .isComplete("passenger_sk_id", "PASSENGER_SK_ID shouldn't have null value")
            .isComplete("train_sk_id", "TRAIN_SK_ID shouldn't have null value")
            .isComplete("class_id", "CLASS_ID shouldn't have null value")
            .isComplete("payment_id", "PAYMENT_ID shouldn't have null value")
            .isComplete("active_status_id", "ACTIVE_STATUS_ID shouldn't have null value")
            .isComplete("day_of_week", "DAY_OF_WEEK shouldn't have null value")
            .isComplete("booking_lead_days", "BOOKING_LEAD_DAYS shouldn't have null value")
            .isComplete("departure_date", "DEPARTURE_DATE shouldn't have null value")
            .isComplete("price", "PRICE shouldn't have null value")
            .isComplete("discount", "DISCOUNT shouldn't have null value")
            .isComplete("final_price", "FINAL_PRICE shouldn't have null value")
            .isComplete("is_weekend", "IS_WEEKEND shouldn't have null value")
        )

        self.run_tests(check)

    def test_uniqueness(self):
        check = (
            Check(self.session, CheckLevel.Error, "Uniqueness Check")
            .isUnique(
                "ticket_id",
                "TICKET_ID must be unique",
            )
        )

        self.run_tests(check)

    def test_numeric(self):
            check = (
                Check(self.session, CheckLevel.Error, "Numeric Validation")
                .hasMin(
                    "price",
                    lambda x: x > 0,
                    "PRICE must be greater than 0",
                )
                .hasMin(
                    "discount",
                    lambda x: x >= 0,
                    "DISCOUNT must not be negative",
                )
                .hasMax(
                    "discount",
                    lambda x: x <= 1,
                    "DISCOUNT must not exceed 1 (100%)",
                )
                .hasMin(
                    "final_price",
                    lambda x: x > 0,
                    "FINAL_PRICE must be greater than 0",
                )
                .hasMin(
                    "booking_lead_days",
                    lambda x: x >= 0,
                    "BOOKING_LEAD_DAYS must not be negative",
                )
                .hasMin(
                    "day_of_week",
                    lambda x: x >= 0,
                    "DAY_OF_WEEK must be within 0-6 range (min)",
                )
                .hasMax(
                    "day_of_week",
                    lambda x: x <= 6,
                    "DAY_OF_WEEK must be within 0-6 range (max)",
                )
            )

            self.run_tests(check)

    def test_consistency(self):
        check = (
            Check(self.session, CheckLevel.Error, "Consistency Validation")
            .satisfies(
                "ABS(final_price - price * (1 - discount)) < 0.01",
                "final_price_matches_price_minus_discount_pct",
                lambda x: x == 1.0,
            )
            .satisfies(
                "final_price <= price",
                "final_price_not_exceed_price",
                lambda x: x == 1.0,
            )
            .satisfies(
                "final_price >= 0",
                "final_price_non_negative",
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