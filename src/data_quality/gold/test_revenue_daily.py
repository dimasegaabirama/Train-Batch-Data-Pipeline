from pydeequ.checks import Check, CheckLevel

from src.data_quality import BaseTest


class TestRevenueDaily(BaseTest):

    stage = "gold"
    table_name = "revenue_daily"

    def test_completeness(self):
        check = (
            Check(self.session, CheckLevel.Error, "RevenueDaily - Completeness Check")
            .isComplete("revenue_date", "REVENUE_DATE shouldn't have null value")
            .isComplete("route_sk_id", "ROUTE_SK_ID shouldn't have null value")
            .isComplete("class_id", "CLASS_ID shouldn't have null value")
            .isComplete("total_tickets", "TOTAL_TICKETS shouldn't have null value")
            .isComplete("gross_revenue", "GROSS_REVENUE shouldn't have null value")
            .isComplete("net_revenue", "NET_REVENUE shouldn't have null value")
        )

        self.run_tests(check)

    def test_grain_uniqueness(self):
        check = (
            Check(self.session, CheckLevel.Error, "RevenueDaily - Grain Uniqueness")
            .isUnique(
                ["revenue_date", "route_sk_id", "class_id"],
                "Combination of revenue_date, route_sk_id, class_id must be unique",
            )
        )

        self.run_tests(check)

    def test_non_negative_values(self):
        check = (
            Check(self.session, CheckLevel.Error, "RevenueDaily - Non-Negative Values")
            .isNonNegative("total_tickets")
            .isNonNegative("gross_revenue")
            .isNonNegative("total_discount")
            .isNonNegative("net_revenue")
            .isNonNegative("refunded_revenue")
            .isNonNegative("avg_ticket_price")
        )

        self.run_tests(check)

    def test_net_revenue_after_refund_range(self):
        check = (
            Check(self.session, CheckLevel.Warning, "RevenueDaily - Net After Refund Range")
            .satisfies(
                "net_revenue_after_refund >= 0",
                "net_revenue_after_refund_non_negative",
                lambda x: x == 1.0,
            )
        )

        self.run_tests(check)

    def test_revenue_calculation_consistency(self):
        check = (
            Check(self.session, CheckLevel.Error, "RevenueDaily - Calculation Consistency")
            .satisfies(
                "ABS(net_revenue - (gross_revenue - total_discount)) < 0.01",
                "net_revenue_matches_gross_minus_discount",
                lambda x: x == 1.0,
            )
            .satisfies(
                "ABS(net_revenue_after_refund - (net_revenue - refunded_revenue)) < 0.01",
                "net_after_refund_matches_calculation",
                lambda x: x == 1.0,
            )
        )

        self.run_tests(check)

    def test_avg_ticket_price_consistency(self):
        check = (
            Check(self.session, CheckLevel.Warning, "RevenueDaily - Avg Ticket Price Consistency")
            .satisfies(
                """
                total_tickets = 0
                OR ABS(avg_ticket_price - (gross_revenue / total_tickets)) < 0.01
                """,
                "avg_ticket_price_matches_calculation",
                lambda x: x == 1.0,
            )
        )

        self.run_tests(check)

    def test_discount_and_refund_bounds(self):
        check = (
            Check(self.session, CheckLevel.Error, "RevenueDaily - Discount and Refund Bounds")
            .satisfies(
                "total_discount <= gross_revenue",
                "discount_not_exceed_gross_revenue",
                lambda x: x == 1.0,
            )
            .satisfies(
                "refunded_revenue <= net_revenue",
                "refunded_not_exceed_net_revenue",
                lambda x: x == 1.0,
            )
        )

        self.run_tests(check)

    def test_dataset(self):
        check = (
            Check(self.session, CheckLevel.Error, "RevenueDaily - Dataset Validation")
            .hasSize(
                lambda x: x > 0,
                "Dataset must not be empty",
            )
        )

        self.run_tests(check)