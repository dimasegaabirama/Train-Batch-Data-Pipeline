from pyspark.sql.session import SparkSession


def initialize_namespace(spark: SparkSession):
    namespaces = ["nessie.bronze", "nessie.silver", "nessie.gold"]
    for ns in namespaces:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {ns}")


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
        PARTITIONED BY (bucket(8, id))
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
            is_active BOOLEAN,
            start_date TIMESTAMP,
            end_date TIMESTAMP
        )
        USING ICEBERG
        """,
        """
        ALTER TABLE nessie.silver.stations
        WRITE ORDERED BY id, start_date
        """,

        # SCD Type 1
        """
        CREATE TABLE IF NOT EXISTS nessie.silver.routes(
            id INT,
            sk_org_station_id BIGINT,
            sk_dest_station_id BIGINT,
            sk_train_id BIGINT,
            distance_km INT,
            duration_minutes INT,
            is_active BOOLEAN
        )
        USING ICEBERG
        """,
        """
        ALTER TABLE nessie.silver.routes
        WRITE ORDERED BY id
        """,

        # SCD Type 1
        """
        CREATE TABLE IF NOT EXISTS nessie.silver.status(
            id INT,
            status STRING
        )
        USING ICEBERG
        """,
        """
        ALTER TABLE nessie.silver.status
        WRITE ORDERED BY id
        """,

        # SCD Type 1
        """
        CREATE TABLE IF NOT EXISTS nessie.silver.class(
            id INT,
            class_name STRING
        )
        USING ICEBERG
        """,
        """
        ALTER TABLE nessie.silver.class
        WRITE ORDERED BY id
        """,

        # SCD Type 1
        """
        CREATE TABLE IF NOT EXISTS nessie.silver.payment(
            id INT,
            method STRING
        )
        USING ICEBERG
        """,
        """
        ALTER TABLE nessie.silver.payment
        WRITE ORDERED BY id
        """,

        # Fact table
        """
        CREATE TABLE IF NOT EXISTS nessie.silver.tickets(
            id INT,
            sk_passenger_id BIGINT,
            sk_train_id BIGINT,
            sk_status_id INT,
            sk_class_id INT,
            sk_payment_id INT,
            route_id INT,
            price DECIMAL(38, 2),
            discount DECIMAL(10, 2),
            final_price DECIMAL(38, 2),
            seat_number STRING,
            source STRING,
            departure_date TIMESTAMP,
            load_at DATE
        )
        USING ICEBERG
        PARTITIONED BY (days(departure_date))
        """,
        """
        ALTER TABLE nessie.silver.tickets
        WRITE ORDERED BY id
        """
    ]
    for table in tables:
        spark.sql(table)