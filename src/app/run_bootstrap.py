import logging

from src.app.bootstrap import *
from src.core.config import Config
from src.core.logger import AppLogger
from src.core.session import Session


def main(session: Session, logger: logging.Logger):

    logger.info("SETUP BRANCH...")
    initialize_branch(session)

    logger.info("SETUP NAMESPACES...")
    initialize_namespace(session)

    logger.info("SETUP TABLES...")
    initialize_table(session)

    logger.info("SETUP SEED...")
    initialize_seed(session)

    logger.info("SETUP BASE TAG...")
    initialize_tag(session)

    logger.info("SETUP SUCCESS")


if __name__ == "__main__":
    config = Config()
    logger = AppLogger.get_logger(type="stream")

    session = Session(logger, config).get_session("dev")

    print(session.conf.get("spark.sql.catalog.nessie.uri"))
    print(session.conf.get("spark.sql.catalog.nessie.ref"))
    print(session.conf.get("spark.sql.catalog.nessie.warehouse"))

    main(session, logger)
