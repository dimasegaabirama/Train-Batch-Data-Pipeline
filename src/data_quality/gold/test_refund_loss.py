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
                "Combination of refund_date, route_sk_id, class_id must be unique"
            )
        )

        self.run_tests(check)

    def test_non_negative_values(self):
        check = (
            Check(self.session, CheckLevel.Error, "RefundLoss - Non-Negative Values")
            .isNonNegative(column="total_tickets_refunded", hint="TOTAL_TICKETS_REFUNDED must be non-negative")
            .isNonNegative(column="total_refund_amount", hint="TOTAL_REFUND_AMOUNT must be non-negative")
            .isNonNegative(column="avg_refund_amount", hint="AVG_REFUND_AMOUNT must be non-negative")
            .isNonNegative(column="avg_days_cancel_to_refund", hint = "AVG_DAYS_CANCEL_TO_REFUND must be non-negative")
            .isNonNegative(column="avg_hours_to_refund", hint="AVG_HOURS_TO_REFUND must be non-negative")
            .isNonNegative(column="avg_days_created_to_refund", hint="AVG_DAYS_CREATED_TO_REFUND must be non-negative")
            .isNonNegative(column="total_refunded_with_promo", hint="TOTAL_REFUNDED_WITH_PROMO must be non-negative")
            .isNonNegative(column="total_refunded_with_family_flag", hint="TOTAL_REFUNDED_WITH_FAMILY_FLAG must be non-negative")
        )

        self.run_tests(check)

    def test_subset_metric_consistency(self):

        check = (
            Check(self.session, CheckLevel.Error, "RefundLoss - Subset Metric Consistency")
            .satisfies(
                "total_refunded_with_promo <= total_tickets_refunded",
                "promo_refund_not_exceed_total",
                lambda x: x == 1.0,
                "TOTAL_REFUNDED_WITH_PROMO should not exceed TOTAL_TICKETS_REFUNDED",
            )
            .satisfies(
                "total_refunded_with_family_flag <= total_tickets_refunded",
                "family_refund_not_exceed_total",
                lambda x: x == 1.0,
                "TOTAL_REFUNDED_WITH_FAMILY_FLAG should not exceed TOTAL_TICKETS_REFUNDED",
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
                "AVG_REFUND_AMOUNT should equal TOTAL_REFUND_AMOUNT divided by TOTAL_TICKETS_REFUNDED",
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
                "AVG_DAYS_CREATED_TO_REFUND should be greater than or equal to AVG_DAYS_CANCEL_TO_REFUND",
            )
        )

        self.run_tests(check)

    def test_dataset(self):
        check = (
            Check(self.session, CheckLevel.Error, "RefundLoss - Dataset Validation")
            .hasSize(
                lambda x: x > 0,
                "Dataset must not be empty"
            )
        )

        self.run_tests(check)