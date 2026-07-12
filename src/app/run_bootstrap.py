from src.app.bootstrap import *
from src.core import AppLogger
from pyspark.sql.session import SparkSession

class PipelineBootstrap:
    def __init__(self, session: SparkSession, logger: AppLogger):
        self.session = session
        self.logger = logger

    def run_bootstrap(self):

        self.logger.info("SETUP BRANCH...")
        initialize_branch(self.session)

        self.logger.info("SETUP NAMESPACES...")
        initialize_namespace(self.session)

        self.logger.info("SETUP TABLES...")
        initialize_table(self.session)

        self.logger.info("SETUP SEED...")
        initialize_seed(self.session)

        self.logger.info("SETUP BASE TAG...")
        initialize_tag(self.session)

        self.logger.info("SETUP SUCCESS")


if __name__ == "__main__":
    pass

