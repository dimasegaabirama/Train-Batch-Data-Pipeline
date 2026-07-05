from typing_extensions import Dict
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from typing_extensions import List, Union

from src.core import Config, AppLogger, resolve_registry_class

from src.models.data_config import StageType
from src.utils.nessie_utils import pipeline_branch
from src.utils.table_utils import create_table_fullname


class PipelineOrchestrator:
    def __init__(
        self,
        logger: AppLogger,
        session: SparkSession,
        config: Config
    ):
        self.logger = logger
        self.session = session
        self.config = config


    # =========================
    # EXTRACT
    # =========================
    def extract(self, stage: StageType, table_name: str) -> DataFrame:

        start_date = self.config.get_start_date()
        end_date = self.config.get_end_date()

        # === Extractor ===
        extractor = resolve_registry_class(
            stage=stage,
            table_name=table_name,
            component_name="extract"
        )

        # === Condition ===
        condition = resolve_registry_class(
            stage=stage,
            table_name=table_name,
            component_name="filter",
            required=False,
        )

        # === Filter ===
        field = self.config.get_filter_field(stage=stage, table_name=table_name)
        if field:
            condition = condition(
                stage=stage, field=field, start_date=start_date, end_date=end_date
            )

        # === Table Schema ===
        table_schema = self.config.get_schema_table(table_name=table_name, stage=stage)

        return extractor(
            stage=stage,
            logger=self.logger,
            session=self.session,
            config=self.config,
            table_name=table_name
            condition=condition,
            table_schema=table_schema
        ).extract()
    

    # =========================
    # TRANSFORM
    # =========================
    def transform(
        self, stage: StageType, dataframe: DataFrame, table_name: str
    ) -> DataFrame:

        lookup_table_name = self.config.get_table_deps(table_name)

        # === Transformer ===
        transformer = resolve_registry_class(
            stage=stage,
            table_name=table_name,
            component_name="transform"
        )

        return transformer(
            logger=self.logger,
            session=self.session,
            config=self.config,
            dataframe=dataframe,
            lookup_table_name=lookup_table_name
        ).transform()

    # =========================
    # LOAD
    # =========================
    def load(
        self,
        stage: StageType,
        stage_target: StageType,
        dataframe: DataFrame,
        table_name: str,
    ) -> DataFrame:
        
        # === Write Mode ===
        write_mode = self.config.get_table_write_mode(table_name=table_name, stage=stage_target)

        # === Query ===
        queries = self.config.get_query_table(table_name=table_name)
        
        # === Loader ===
        loader = resolve_registry_class(
            stage=stage,
            table_name=table_name,
            component_name="load"
        )

        return loader(
            stage=stage_target,
            logger=self.logger,
            session=self.session,
            config=self.config,
            table_name=table_name,
            write_mode=write_mode,
            query=
            dataframe=dataframe
        ).load()

    # =========================
    # SINGLE TABLE PIPELINE
    # =========================
    @pipeline_branch
    def run_table(self, stage: StageType, table_name: str) -> None:

        self.logger.info(f"[{stage}] Start pipeline: {table_name}")

        # === STAGE TARGET ===
        stage_target = self.config.get_schema_downstream(stage=stage)

        # === EXTRACT ===
        self.logger.info(f"[{stage}] Extract Table: {table_name}")
        extract_stage = self.extract(stage=stage, table_name=table_name)

        # === TRANSFORM ===
        self.logger.info(f"[{stage}] Transform Table: {table_name}")
        transform_stage = self.transform(
            stage=stage, dataframe=extract_stage, table_name=table_name
        )

        # === LOAD ===
        self.logger.info(f"[{stage}] Load Table: {table_name}")
        self.load(
            stage=stage,
            stage_target=stage_target,
            dataframe=transform_stage,
            table_name=table_name
        )

        self.logger.info(f"[{stage}] Finished: {table_name} ✅\n")

    def run_all_tables(self, stage: str, table_names: List[str]) -> None:
        for table_name in table_names:
            self.run_table(stage=stage, table_name=table_name)


if __name__ == "__main__":
    pass
