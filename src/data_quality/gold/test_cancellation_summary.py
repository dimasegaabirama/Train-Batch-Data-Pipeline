from pydeequ.checks import Check, CheckLevel

from src.data_quality import BaseTest


class TestCancellationSummary(BaseTest):

    def test_completeness(self):
        check = (
            Check(self.session, CheckLevel.Error, "CancellationSummary - Completeness Check")
            .isComplete("booking_date", "BOOKING_DATE shouldn't have null value")
            .isComplete("route_sk_id", "ROUTE_SK_ID shouldn't have null value")
            .isComplete("class_id", "CLASS_ID shouldn't have null value")
            .isComplete("total_ticket_id", "TOTAL_TICKET_ID shouldn't have null value")
            .isComplete("total_created", "TOTAL_CREATED shouldn't have null value")
            .isComplete("total_paid", "TOTAL_PAID shouldn't have null value")
            .isComplete("total_cancelled", "TOTAL_CANCELLED shouldn't have null value")
            .isComplete("cancellation_rate", "CANCELLATION_RATE shouldn't have null value")
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
            .isNonNegative("total_ticket_id")
            .isNonNegative("total_created")
            .isNonNegative("total_paid")
            .isNonNegative("total_cancelled")
            .isNonNegative("total_refunded")
            .isNonNegative("cancel_before_payment")
            .isNonNegative("cancel_after_payment")
            .isNonNegative("lost_revenue")
            .isNonNegative("avg_hours_to_cancel")
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
                "total_cancelled = cancel_before_payment + cancel_after_payment",
                "cancelled_breakdown_matches_total",
                lambda x: x == 1.0,
            )
            .satisfies(
                "total_created >= total_paid + total_cancelled",
                "created_not_exceeded_by_paid_plus_cancelled",
                lambda x: x == 1.0,
            )
            .satisfies(
                "total_refunded <= total_cancelled",
                "refunded_not_exceed_cancelled",
                lambda x: x == 1.0,
            )
            .satisfies(
                "total_cancelled <= total_created",
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