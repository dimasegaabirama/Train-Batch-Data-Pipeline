import pyspark.sql.functions as F
from pyspark.sql.column import Column


def build_mongo_incremental_filter(field: str, start_date: str, end_date: str):
    """
    Build a MongoDB aggregation filter for bronze-stage extraction.

    This filter is intended for source-level incremental extraction
    using MongoDB aggregation pushdown.

    The generated condition applies:

        start_date <= field < end_date

    Parameters
    ----------
    field : str
        Name of the date field used for filtering.

    start_date : str
        Start date (inclusive).

    end_date : str
        End date (exclusive).

    Returns
    -------
    list
        MongoDB aggregation pipeline containing a ``$match`` stage.
    """

    return [
        {
            "$match": {
                "$expr": {
                    "$and": [
                        {"$gte": [{"$toDate": f"${field}"}, start_date]},
                        {"$lt": [{"$toDate": f"${field}"}, end_date]},
                    ]
                }
            }
        }
    ]


def build_iceberg_incremental_filter(
    field: str, start_date: str, end_date: str
) -> Column:
    """
    Build a Spark filter expression for silver-stage incremental extraction.

    This filter is typically used when reading data from Iceberg tables
    in the silver layer.

    The generated condition applies:

        start_date <= field < end_date

    Parameters
    ----------
    field : str
        Name of the date column used for filtering.

    start_date : str
        Start date (inclusive).

    end_date : str
        End date (exclusive).

    Returns
    -------
    Column
        Spark SQL filter expression.
    """

    start_date = F.to_date(F.lit(start_date))
    end_date = F.to_date(F.lit(end_date))

    return (F.col(field) >= F.lit(start_date)) & (F.col(field) < F.lit(end_date))
