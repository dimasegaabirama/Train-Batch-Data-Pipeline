from pydeequ.checks import Check, CheckLevel

from src.data_quality import BaseTest


class TestTrainPerformance(BaseTest):

    def test_completeness(self):
        check = (
            Check(self.session, CheckLevel.Error, "TrainPerformance - Completeness Check")
            .isComplete("departure_date", "DEPARTURE_DATE shouldn't have null value")
            .isComplete("train_sk_id", "TRAIN_SK_ID shouldn't have null value")
            .isComplete("name", "NAME shouldn't have null value")
            .isComplete("capacity", "CAPACITY shouldn't have null value")
            .isComplete("total_tickets_sold", "TOTAL_TICKETS_SOLD shouldn't have null value")
            .isComplete("net_tickets_sold", "NET_TICKETS_SOLD shouldn't have null value")
            .isComplete("occupancy_rate", "OCCUPANCY_RATE shouldn't have null value")
            .isComplete("is_fully_booked", "IS_FULLY_BOOKED shouldn't have null value")
        )

        self.run_tests(check)

    def test_grain_uniqueness(self):
        check = (
            Check(self.session, CheckLevel.Error, "TrainPerformance - Grain Uniqueness")
            .isUnique(
                ["departure_date", "train_sk_id"],
                "Combination of departure_date, train_sk_id must be unique",
            )
        )

        self.run_tests(check)

    def test_non_negative_values(self):
        check = (
            Check(self.session, CheckLevel.Error, "TrainPerformance - Non-Negative Values")
            .isNonNegative(column="capacity", hint="CAPACITY must be non-negative")
            .isNonNegative(column="total_tickets_sold", hint="TOTAL_TICKETS_SOLD must be non-negative")
            .isNonNegative(column="total_cancelled_tickets", hint="TOTAL_CANCELLED_TICKETS must be non-negative")
            .isNonNegative(column="net_tickets_sold", hint="NET_TICKETS_SOLD must be non-negative")
            .isNonNegative(column="total_revenue", hint="TOTAL_REVENUE must be non-negative")
            .isNonNegative(column="family_ticket_count", hint="FAMILY_TICKET_COUNT must be non-negative")
            .isNonNegative(column="promo_ticket_count", hint="PROMO_TICKET_COUNT must be non-negative")
        )

        self.run_tests(check)

    def test_capacity_valid(self):
        check = (
            Check(self.session, CheckLevel.Error, "TrainPerformance - Capacity Validity")
            .satisfies(
                "capacity > 0",
                "capacity_greater_than_zero",
                lambda x: x == 1.0,
                "CAPACITY should be greater than zero"
            )
        )

        self.run_tests(check)

    def test_ticket_count_consistency(self):
        check = (
            Check(self.session, CheckLevel.Error, "TrainPerformance - Ticket Count Consistency")
            .satisfies(
                "net_tickets_sold = total_tickets_sold - total_cancelled_tickets",
                "net_tickets_matches_calculation",
                lambda x: x == 1.0,
                "NET_TICKETS_SOLD should equal TOTAL_TICKETS_SOLD minus TOTAL_CANCELLED_TICKETS"
            )
            .satisfies(
                "net_tickets_sold <= capacity",
                "net_tickets_not_exceed_capacity",
                lambda x: x == 1.0,
                "NET_TICKETS_SOLD should not exceed CAPACITY"
            )
            .satisfies(
                "total_cancelled_tickets <= total_tickets_sold",
                "cancelled_not_exceed_total_sold",
                lambda x: x == 1.0,
                "TOTAL_CANCELLED_TICKETS should not exceed TOTAL_TICKETS_SOLD"
            )
        )

        self.run_tests(check)

    def test_subset_metric_consistency(self):
        check = (
            Check(self.session, CheckLevel.Error, "TrainPerformance - Subset Metric Consistency")
            .satisfies(
                "family_ticket_count <= total_tickets_sold",
                "family_count_not_exceed_total",
                lambda x: x == 1.0,
                "FAMILY_TICKET_COUNT should not exceed TOTAL_TICKETS_SOLD"
            )
            .satisfies(
                "promo_ticket_count <= total_tickets_sold",
                "promo_count_not_exceed_total",
                lambda x: x == 1.0,
                "PROMO_TICKET_COUNT should not exceed TOTAL_TICKETS_SOLD"
            )
        )

        self.run_tests(check)

    def test_occupancy_rate_range(self):
        check = (
            Check(self.session, CheckLevel.Error, "TrainPerformance - Occupancy Rate Range")
            .satisfies(
                "occupancy_rate >= 0.0 AND occupancy_rate <= 1.0",
                "occupancy_rate_in_range",
                lambda x: x == 1.0,
                "OCCUPANCY_RATE should be between 0.0 and 1.0"
            )
        )

        self.run_tests(check)

    def test_occupancy_rate_consistency(self):
        check = (
            Check(self.session, CheckLevel.Warning, "TrainPerformance - Occupancy Rate Consistency")
            .satisfies(
                """
                capacity = 0
                OR ABS(occupancy_rate - (net_tickets_sold / capacity)) < 0.001
                """,
                "occupancy_rate_matches_calculation",
                lambda x: x == 1.0,
                "OCCUPANCY_RATE should equal NET_TICKETS_SOLD divided by CAPACITY"
            )
        )

        self.run_tests(check)

    def test_is_fully_booked_consistency(self):
        check = (
            Check(self.session, CheckLevel.Warning, "TrainPerformance - Fully Booked Consistency")
            .satisfies(
                """
                (is_fully_booked = true AND net_tickets_sold = capacity)
                OR (is_fully_booked = false AND net_tickets_sold < capacity)
                """,
                "is_fully_booked_matches_ticket_count",
                lambda x: x == 1.0,
                "IS_FULLY_BOOKED should be true if NET_TICKETS_SOLD equals CAPACITY, and false otherwise"
            )
        )

        self.run_tests(check)

    def test_type_enum(self):
        """
        PERLU DIKONFIRMASI: allowed_values untuk kolom `type` -- saya kosongkan
        dulu placeholder-nya, isi sesuai kategori train yang valid di sistem kamu
        (misal: economy, business, executive, dsb).
        """
        check = (
            Check(self.session, CheckLevel.Warning, "TrainPerformance - Type Enum Validation")
            .isContainedIn(
                column="type",
                allowed_values=["economy", "business", "executive"],  # <- sesuaikan
                hint="Invalid train type",
            )
        )

        self.run_tests(check)

    def test_dataset(self):
        check = (
            Check(self.session, CheckLevel.Error, "TrainPerformance - Dataset Validation")
            .hasSize(
                lambda x: x > 0,
                "Dataset must not be empty",
                "TrainPerformance dataset should contain at least one record"
            )
        )

        self.run_tests(check)