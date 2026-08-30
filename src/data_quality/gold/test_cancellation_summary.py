from pydeequ.checks import Check, CheckLevel

from src.data_quality import BaseTest


class TestCancellationSummary(BaseTest):

        # booking_date DATE,
        # route_sk_id BIGINT,
        # class_id INT,

        # total_tickets INT,
        # total_tickets_paid INT,
        # total_tickets_cancelled INT,
        # total_tickets_refunded INT,

        # cancelled_before_payment INT,
        # cancelled_after_payment INT,
        # cancelled_not_yet_refunded INT,

        # total_revenue_lost DECIMAL(18, 2),
        # avg_hours_to_cancel DOUBLE,
        # cancellation_rate DOUBLE,
        # cancelled_after_payment_rate DOUBLE,
        # updated_at TIMESTAMP


    def test_completeness(self):
        check = (
            Check(self.session, CheckLevel.Error, "CancellationSummary - Completeness Check")
            .isComplete("booking_date", "BOOKING_DATE shouldn't have null value")
            .isComplete("route_sk_id", "ROUTE_SK_ID shouldn't have null value")
            .isComplete("class_id", "CLASS_ID shouldn't have null value")
            .isComplete("total_tickets", "TOTAL_TICKETS shouldn't have null value")
            .isComplete("total_tickets_created", "TOTAL_TICKETS_CREATED shouldn't have null value")
            .isComplete("total_tickets_paid", "TOTAL_TICKETS_PAID shouldn't have null value")
            .isComplete("total_tickets_cancelled", "TOTAL_TICKETS_CANCELLED shouldn't have null value")
            .isComplete("total_tickets_refunded", "TOTAL_TICKETS_REFUNDED shouldn't have null value")
            .isComplete("cancelled_before_payment", "CANCELLED_BEFORE_PAYMENT shouldn't have null value")
            .isComplete("cancelled_after_payment", "CANCELLED_AFTER_PAYMENT shouldn't have null value")
            .isComplete("cancelled_not_yet_refunded", "CANCELLED_NOT_YET_REFUNDED shouldn't have null value")
            .isComplete("total_revenue_lost", "TOTAL_REVENUE_LOST shouldn't have null value")
            .isComplete("avg_hours_to_cancel", "AVG_HOURS_TO_CANCEL shouldn't have null value")
            .isComplete("cancellation_rate", "CANCELLATION_RATE shouldn't have null value")
            .isComplete("cancelled_after_payment_rate", "CANCELLED_AFTER_PAYMENT_RATE shouldn't have null value")
        )

        self.run_tests(check)

    def test_grain_uniqueness(self):
        check = (
            Check(self.session, CheckLevel.Error, "CancellationSummary - Grain Uniqueness")
            .isUnique(
                ["booking_date", "route_sk_id", "class_id"],
                "Combination of booking_date, route_sk_id, class_id must be unique",
            )
        )

        self.run_tests(check)

    def test_non_negative_values(self):
        check = (
            Check(self.session, CheckLevel.Error, "CancellationSummary - Non-Negative Values")
            .isNonNegative(column="total_tickets", hint="TOTAL_TICKETS must be non-negative")
            .isNonNegative(column="total_tickets_created", hint="TOTAL_TICKETS_CREATED must be non-negative")
            .isNonNegative(column="total_tickets_paid", hint="TOTAL_TICKETS_PAID must be non-negative")
            .isNonNegative(column="total_tickets_cancelled", hint="TOTAL_TICKETS_CANCELLED must be non-negative")
            .isNonNegative(column="total_tickets_refunded", hint="TOTAL_TICKETS_REFUNDED must be non-negative")
            .isNonNegative(column="cancelled_before_payment", hint="CANCELLED_BEFORE_PAYMENT must be non-negative")
            .isNonNegative(column="cancelled_after_payment", hint="CANCELLED_AFTER_PAYMENT must be non-negative")
            .isNonNegative(column="cancelled_not_yet_refunded", hint="CANCELLED_NOT_YET_REFUNDED must be non-negative")
            .isNonNegative(column="total_revenue_lost", hint="TOTAL_REVENUE_LOST must be non-negative")
            .isNonNegative(column="avg_hours_to_cancel", hint="AVG_HOURS_TO_CANCEL must be non-negative")
            .isNonNegative(column="cancelled_after_payment_rate", hint="CANCELLED_AFTER_PAYMENT_RATE must be non-negative")
        )

        self.run_tests(check)

    def test_cancellation_rate_range(self):
        check = (
            Check(self.session, CheckLevel.Error, "CancellationSummary - Rate Range")
            .satisfies(
                "cancellation_rate >= 0 AND cancellation_rate <= 100",
                "cancellation_rate_in_range",
                lambda x: x == 1.0,
            )
        )

        self.run_tests(check)

    def test_metric_consistency(self):
        check = (
            Check(self.session, CheckLevel.Error, "CancellationSummary - Metric Consistency")
            .satisfies(
                "total_tickets_cancelled = total_tickets_cancelled_before_payment + total_tickets_cancelled_after_payment",
                "cancelled_breakdown_matches_total",
                lambda x: x == 1.0,
            )
            .satisfies(
                "total_tickets_created >= total_tickets_paid + total_tickets_cancelled",
                "created_not_exceeded_by_paid_plus_cancelled",
                lambda x: x == 1.0,
            )
            .satisfies(
                "total_tickets_refunded <= total_tickets_cancelled",
                "refunded_not_exceed_cancelled",
                lambda x: x == 1.0,
            )
            .satisfies(
                "total_tickets_cancelled <= total_tickets_created",
                "cancelled_not_exceed_created",
                lambda x: x == 1.0,
            )
        )

        self.run_tests(check)

    def test_dataset(self):
        check = (
            Check(self.session, CheckLevel.Error, "CancellationSummary - Dataset Validation")
            .hasSize(
                lambda x: x > 0,
                "Dataset must not be empty",
            )
        )

        self.run_tests(check)