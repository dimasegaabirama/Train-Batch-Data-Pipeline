from pydeequ.checks import Check, CheckLevel

from src.data_quality import BaseTest


class TestTickets(BaseTest):

    stage = "silver"
    table_name = "tickets"


#  |-- ticket_id: string (nullable = true)
#  |-- route_sk_id: long (nullable = true)
#  |-- passenger_sk_id: long (nullable = true)
#  |-- train_sk_id: long (nullable = true)
#  |-- class_id: integer (nullable = true)
#  |-- payment_id: integer (nullable = true)
#  |-- active_status_id: integer (nullable = true)
#  |-- day_of_week: integer (nullable = true)
#  |-- booking_lead_days: integer (nullable = true)
#  |-- departure_date: timestamp (nullable = true)
#  |-- paid_at: timestamp (nullable = true)
#  |-- cancelled_at: timestamp (nullable = true)
#  |-- refunded_at: timestamp (nullable = true)
#  |-- created_at: timestamp (nullable = true)
#  |-- price: decimal(18,2) (nullable = true)
#  |-- discount: decimal(18,2) (nullable = true)
#  |-- final_price: decimal(18,2) (nullable = true)
#  |-- family_flag: boolean (nullable = true)
#  |-- has_child: boolean (nullable = true)
#  |-- has_promo: boolean (nullable = true)
#  |-- is_weekend: boolean (nullable = true)

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
            .isComplete("day_of_week", "day_of_week shouldn't have null value")
            .isComplete("booking_lead_days", "booking_lead_days shouldn't have null value")
            .isComplete("departure_date", "departure_date shouldn't have null value")
            .isComplete("price", "price shouldn't have null value")
            .isComplete("discount", "discount shouldn't have null value")
            .isComplete("final_price", "final_price shouldn't have null value")
            .isComplete("is_weekend", "is_weekend shouldn't have null value")
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
                "type",
                [
                    "economy",
                    "business",
                    "executive",
                    "unknown"
                ],
                "Invalid train type",
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