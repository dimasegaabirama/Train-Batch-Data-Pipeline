from functools import wraps
from logging import Logger

from pyspark.sql.session import SparkSession
from src.core.config import CatalogManager


def pipeline_branch(func):
    """
    Decorator that wraps a pipeline function inside a Nessie branch lifecycle:
      1. Create a temporary branch from main
      2. Execute the pipeline function on that branch
      3. Merge the branch back into main
      4. Drop the temporary branch (always, even on failure)
    """

    @wraps(func)
    def wrapper(*args, **kwargs):
        self = args[0]
        logger: Logger = self.logger
        session: SparkSession = self.session
        
        stage: str = kwargs["stage"]
        catalog: str = CatalogManager().get_catalog_name()
        table_name: str = kwargs["table_name"]

        branch_name = f"{stage}_{table_name}"

        logger.info(f"[NESSIE] Starting stage = {stage}, table = {table_name}")
        logger.info(f"[NESSIE] Creating temp branch '{branch_name}' from main")

        try:
            session.sql(f"CREATE BRANCH {branch_name} IN {catalog} FROM main")
            session.sql(f"USE REFERENCE {branch_name} IN {catalog}")

            func(*args, **kwargs)

            logger.info(f"[NESSIE] Merging '{branch_name}' back to main")
            session.sql(f"MERGE BRANCH {branch_name} INTO main IN {catalog}")
            logger.info(f"[NESSIE] Stage = {branch_name} finished successfully")

        except Exception as e:
            logger.error(
                f"[NESSIE] Pipeline failed on branch '{branch_name}': {e}",
                exc_info=True,
            )
            raise

        finally:
            try:
                session.sql(f"USE REFERENCE main IN {catalog}")
            except Exception:
                pass

            try:
                logger.info(f"[NESSIE] Dropping temp branch '{branch_name}'")
                session.sql(f"DROP BRANCH {branch_name} IN {catalog}")
            except Exception as e:
                logger.warning(
                    f"[NESSIE] Failed to drop temp branch '{branch_name}': {e}"
                )

    return wrapper
