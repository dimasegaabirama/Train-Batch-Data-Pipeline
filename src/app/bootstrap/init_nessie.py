from pyspark.sql.session import SparkSession


def initialize_branch(spark: SparkSession):
    try:
        spark.sql("CREATE BRANCH IF NOT EXISTS main IN nessie")
        spark.sql("USE REFERENCE main IN nessie")
    except Exception as e:
        raise RuntimeError(f"Failed to initialize branch: {e}")


def initialize_tag(spark: SparkSession):
    try:
        spark.sql("CREATE TAG IF NOT EXISTS baseline_version IN nessie FROM main")
    except Exception as e:
        raise RuntimeError(f"Failed to initialize tag: {e}")
