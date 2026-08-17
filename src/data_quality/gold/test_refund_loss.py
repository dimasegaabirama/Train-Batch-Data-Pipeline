from pydeequ.checks import Check, CheckLevel

from src.data_quality import BaseTest


class TestRefundLoss(BaseTest):

    def test_completeness(self):
        check = (
            Check(self.session, CheckLevel.Error, "RefundLoss - Completeness Check")
            .isComplete("refund_date", "REFUND_DATE shouldn't have null value")
            .isComplete("route_sk_id", "ROUTE_SK_ID shouldn't have null value")
            .isComplete("class_id", "CLASS_ID shouldn't have null value")
            .isComplete("total_tickets_refunded", "TOTAL_TICKETS_REFUNDED shouldn't have null value")
            .isComplete("total_refund_amount", "TOTAL_REFUND_AMOUNT shouldn't have null value")
        )

        self.run_tests(check)

    def test_grain_uniqueness(self):

        check = (
            Check(self.session, CheckLevel.Error, "RefundLoss - Grain Uniqueness")
            .isUnique(
                ["refund_date", "route_sk_id", "class_id"],
                "Combination of refund_date, route_sk_id, class_id must be unique",
            )
        )

        self.run_tests(check)

    def test_non_negative_values(self):
        check = (
            Check(self.session, CheckLevel.Error, "RefundLoss - Non-Negative Values")
            .isNonNegative("total_tickets_refunded")
            .isNonNegative("total_refund_amount")
            .isNonNegative("avg_refund_amount")
            .isNonNegative("avg_days_cancel_to_refund")
            .isNonNegative("avg_hours_to_refund")
            .isNonNegative("avg_days_created_to_refund")
            .isNonNegative("total_refunded_with_promo")
            .isNonNegative("total_refunded_with_family_flag")
        )

        self.run_tests(check)

    def test_subset_metric_consistency(self):

        check = (
            Check(self.session, CheckLevel.Error, "RefundLoss - Subset Metric Consistency")
            .satisfies(
                "total_refunded_with_promo <= total_tickets_refunded",
                "promo_refund_not_exceed_total",
                lambda x: x == 1.0,
            )
            .satisfies(
                "total_refunded_with_family_flag <= total_tickets_refunded",
                "family_refund_not_exceed_total",
                lambda x: x == 1.0,
            )
        )

        self.run_tests(check)

    def test_avg_refund_amount_consistency(self):

        check = (
            Check(self.session, CheckLevel.Error, "RefundLoss - Average Amount Consistency")
            .satisfies(
                """
                total_tickets_refunded = 0
                OR ABS(avg_refund_amount - (total_refund_amount / total_tickets_refunded)) < 0.01
                """,
                "avg_refund_amount_matches_calculation",
                lambda x: x == 1.0,
            )
        )

        self.run_tests(check)

    def test_time_duration_logic(self):

        check = (
            Check(self.session, CheckLevel.Warning, "RefundLoss - Time Duration Logic")
            .satisfies(
                "avg_days_created_to_refund >= avg_days_cancel_to_refund",
                "created_to_refund_gte_cancel_to_refund",
                lambda x: x == 1.0,
            )
        )

        self.run_tests(check)

    def test_dataset(self):
        check = (
            Check(self.session, CheckLevel.Error, "RefundLoss - Dataset Validation")
            .hasSize(
                lambda x: x > 0,
                "Dataset must not be empty",
            )
        )

        self.run_tests(check)