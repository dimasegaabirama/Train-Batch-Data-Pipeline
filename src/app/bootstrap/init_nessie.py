from pyspark.sql.session import SparkSession


def initialize_branch(spark: SparkSession):
    spark.sql("CREATE BRANCH IF NOT EXISTS main IN nessie")
    spark.sql("USE REFERENCE main IN nessie")


def initialize_tag(spark: SparkSession):
    spark.sql("CREATE TAG IF NOT EXISTS baseline_version IN nessie FROM main")
