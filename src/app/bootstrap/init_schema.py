from pyspark.sql.session import SparkSession


def initialize_namespace(spark: SparkSession):
    namespaces = ["nessie.bronze", "nessie.silver", "nessie.gold"]
    for ns in namespaces:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")


def initialize_seed(spark: SparkSession):
    seeds = ["""
        INSERT INTO nessie.silver.status (id, status)
        VALUES 
            (1, 'paid'),
            (2, 'unpaid'),
            (3, 'cancelled'),
            (4, 'refunded')
        """,
        """
        INSERT INTO nessie.silver.class (id, class_name)
        VALUES 
            (1, 'vip'),
            (2, 'family'),
            (3, 'regular'),
            (4, 'promo')
        """,
        """
        INSERT INTO nessie.silver.payment (id, method)
        VALUES 
            (1, 'credit_card'),
            (2, 'debit_card'),
            (3, 'e_wallet'),
            (4, 'bank_transfer'),
            (5, 'cash')
        """
    ]
    for seed in seeds:
        spark.sql(seed)


def initialize_table(spark: SparkSession):
    tables = [
        # ============ BRONZE ============
        """
        CREATE TABLE IF NOT EXISTS nessie.bronze.passengers(
            id INT,
            name STRING,
            gender STRING,
            phone STRING,
            email STRING,
            updated_at TIMESTAMP,
            created_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (days(updated_at))
        """,
        """
        CREATE TABLE IF NOT EXISTS nessie.bronze.trains(
            id INT,
            name STRING,
            type STRING,
            capacity INT,
            updated_at TIMESTAMP,
            created_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (days(updated_at))
        """,
        """
        CREATE TABLE IF NOT EXISTS nessie.bronze.stations(
            id INT,
            name STRING,
            city STRING,
            code STRING,
            updated_at TIMESTAMP,
            created_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (days(updated_at))
        """,
        """
        CREATE TABLE IF NOT EXISTS nessie.bronze.routes(
            id INT,
            origin STRING,
            destination STRING,
            train_id INT,
            distance_km INT,
            duration_minutes INT,
            updated_at TIMESTAMP,
            created_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (days(updated_at))
        """,
        """
        CREATE TABLE IF NOT EXISTS nessie.bronze.tickets(
            id INT,
            ticket_id STRING,
            route_id INT,
            passenger_id INT,
            train_id INT,
            discount DECIMAL(10, 2),
            price DECIMAL(38, 2),
            class STRING,
            seat_number STRING,
            status STRING,
            departure_date STRING,
            extra_info STRUCT<
                child_discount: BOOLEAN,
                family_members: INT,
                promo_code: STRING,
                source: STRING
            >,
            payment STRUCT<
                method: STRING,
                bank: STRING,
                provider: STRING
            >,
            addons ARRAY<STRING>,
            created_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (days(created_at))
        """,

        # ============ SILVER ============

        # SCD Type 2
        """
        CREATE TABLE IF NOT EXISTS nessie.silver.passengers(
            sk_id BIGINT,
            id INT,
            name STRING,
            gender STRING,
            phone STRING,
            email STRING,
            is_active BOOLEAN,
            start_date TIMESTAMP,
            end_date TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (days(start_date), bucket(8, sk_id))
        """,
        """
        ALTER TABLE nessie.silver.passengers
        WRITE ORDERED BY id, start_date
        """,
        """
        CREATE TABLE IF NOT EXISTS nessie.silver.trains(
            sk_id BIGINT,
            id INT,
            name STRING,
            type STRING,
            capacity INT,
            is_active BOOLEAN,
            start_date TIMESTAMP,
            end_date TIMESTAMP
        )
        USING ICEBERG
        """,
        """
        ALTER TABLE nessie.silver.trains
        WRITE ORDERED BY id, start_date
        """,
        """
        CREATE TABLE IF NOT EXISTS nessie.silver.stations(
            sk_id BIGINT,
            id INT,
            name STRING,
            city STRING,
            code STRING,
            is_deleted BOOLEAN
        )
        USING ICEBERG
        """,
        """
        ALTER TABLE nessie.silver.stations
        WRITE ORDERED BY id
        """,

        # SCD Type 1
        """
        CREATE TABLE IF NOT EXISTS nessie.silver.routes(
            sk_id BIGINT,
            id INT,
            sk_org_station_id BIGINT,
            sk_dest_station_id BIGINT,
            sk_train_id BIGINT,
            distance_km INT,
            duration_minutes INT,
            is_deleted BOOLEAN
        )
        USING ICEBERG
        """,
        """
        ALTER TABLE nessie.silver.routes
        WRITE ORDERED BY id
        """,

        # Lookup table - kecil, tidak perlu global sort order
        """
        CREATE TABLE IF NOT EXISTS nessie.silver.status(
            id INT,
            status STRING
        )
        USING ICEBERG
        """,
        """
        ALTER TABLE nessie.silver.status
        WRITE UNORDERED
        """,

        # Lookup table - kecil, tidak perlu global sort order
        """
        CREATE TABLE IF NOT EXISTS nessie.silver.class(
            id INT,
            class_name STRING
        )
        USING ICEBERG
        """,
        """
        ALTER TABLE nessie.silver.class
        WRITE UNORDERED
        """,

        # Lookup table - kecil, tidak perlu global sort order
        """
        CREATE TABLE IF NOT EXISTS nessie.silver.payment(
            id INT,
            method STRING
        )
        USING ICEBERG
        """,
        """
        ALTER TABLE nessie.silver.payment
        WRITE UNORDERED
        """,

        # Fact table
        """
        CREATE TABLE IF NOT EXISTS nessie.silver.tickets(
            ticket_id         STRING,
            route_sk_id       BIGINT,
            passenger_sk_id   BIGINT,
            train_sk_id       BIGINT,

            class_id          INT,
            payment_id        INT,
            active_status_id  INT,

            day_of_week       TINYINT,
            booking_lead_days INT,

            departure_date    TIMESTAMP,
            paid_at           TIMESTAMP,
            cancelled_at      TIMESTAMP,
            refunded_at       TIMESTAMP,
            created_at        TIMESTAMP,

            price             DECIMAL(18, 2),
            discount          DECIMAL(18, 2),
            final_price       DECIMAL(18, 2),

            family_flag       BOOLEAN,
            has_child         BOOLEAN,
            has_promo         BOOLEAN,
            is_weekend        BOOLEAN
        )
        USING ICEBERG
        PARTITIONED BY (month(created_at), bucket(8, passenger_sk_id))
        """,
        """
        ALTER TABLE nessie.silver.tickets
        WRITE ORDERED BY ticket_id
        """,
        """
        CREATE TABLE IF NOT EXISTS nessie.gold.cancellation_summary(
            booking_date TIMESTAMP,
            route_sk_id BIGINT,
            class_id INT,

            total_tickets_created BIGINT,
            total_tickets_paid BIGINT,
            total_tickets_cancelled BIGINT,
            total_tickets_refunded BIGINT,

            cancelled_before_payment BIGINT,
            cancelled_after_payment BIGINT,
            cancelled_not_yet_refunded BIGINT,

            total_revenue_lost DECIMAL(18, 2),
            avg_hours_to_cancel DOUBLE,

            cancellation_rate DOUBLE,
            cancelled_after_payment_rate DOUBLE,

            updated_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (month(booking_date))
        """,
        """
        ALTER TABLE nessie.gold.cancellation_summary
        WRITE ORDERED BY booking_date
        """,
        """
        CREATE TABLE IF NOT EXISTS nessie.gold.revenue_daily(
            revenue_date TIMESTAMP,
            route_sk_id BIGINT,
            class_id INT,

            total_tickets BIGINT,
            gross_revenue DECIMAL(18, 2),
            total_discount_calculated DECIMAL(18, 2),
            net_revenue DECIMAL(18, 2),
            refunded_revenue DECIMAL(18, 2),

            net_revenue_after_refund DECIMAL(18, 2),
            avg_ticket_price DECIMAL(18, 2),

            updated_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (month(revenue_date))
        """,
        """
        ALTER TABLE nessie.gold.revenue_daily
        WRITE ORDERED BY revenue_date
        """,

        """
        CREATE TABLE IF NOT EXISTS nessie.gold.refund_loss(
            refund_date TIMESTAMP,
            route_sk_id BIGINT,
            class_id INT,
            total_tickets_refunded BIGINT,
            total_refund_amount DECIMAL(18, 2),
            avg_refund_amount DECIMAL(18, 2),
            avg_days_cancel_to_refund DOUBLE,
            avg_hours_to_refund DOUBLE,
            avg_days_created_to_refund DOUBLE,
            total_refunded_with_promo BIGINT,
            total_refunded_with_family_flag BIGINT,
            updated_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (month(refund_date))
        """,
        """
        ALTER TABLE nessie.gold.refund_loss
        WRITE ORDERED BY refund_date
        """,

        """
        CREATE TABLE IF NOT EXISTS nessie.gold.train_performance(
            departure_date TIMESTAMP,
            train_sk_id BIGINT,
            name STRING,
            type STRING,
            capacity INT,
            total_tickets_sold BIGINT,
            total_cancelled_tickets BIGINT,
            net_tickets_sold BIGINT,
            total_revenue DECIMAL(18, 2),
            family_ticket_count BIGINT,
            promo_ticket_count BIGINT,
            cancelled_after_departure_flag BOOLEAN,
            occupancy_rate DOUBLE,
            is_fully_booked BOOLEAN,
            updated_at TIMESTAMP
        )
        USING ICEBERG
        PARTITIONED BY (month(departure_date))
        """
    ]
    for table in tables:
        spark.sql(table)