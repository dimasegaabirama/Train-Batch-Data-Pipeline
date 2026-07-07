from typing import List

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from src.core import Config, AppLogger, resolve_registry_class
from src.models.data_config import StageType
from src.utils.nessie_utils import pipeline_branch


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

    def get_formated_query(self, table_name: str, **kwargs):
        return [
            query.format(**kwargs)
            for query in self.config.get_query_table(table_name=table_name)
        ]

    def _resolve(
        self,
        stage: StageType,
        table_name: str,
        component_name: str,
        required: bool = True,
    ):
        """Small helper to centralize registry resolution + logging."""
        return resolve_registry_class(
            stage=stage,
            table_name=table_name,
            component_name=component_name,
            required=required,
        )

    # =========================
    # EXTRACT
    # =========================
    def extract(self, stage: StageType, table_name: str) -> DataFrame:

        start_date = self.config.get_start_date()
        end_date = self.config.get_end_date()

        # === Extractor ===
        extractor = self._resolve(stage, table_name, "extract")

        # === Condition (filter) ===
        condition_cls = self._resolve(stage, table_name, "filter", required=False)

        field = self.config.get_filter_field(stage=stage, table_name=table_name)
        condition = (
            condition_cls(
                stage=stage,
                field=field,
                start_date=start_date,
                end_date=end_date,
            )
            if condition_cls is not None
            else None
        )

        # === Table Schema ===
        table_schema = self.config.get_schema_table(table_name=table_name, stage=stage)

        return extractor(
            stage=stage,
            logger=self.logger,
            session=self.session,
            config=self.config,
            table_name=table_name,
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
        transformer = self._resolve(stage, table_name, "transform")

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
        dataframe: DataFrame,
        table_name: str,
        table_view_name: str
    ) -> DataFrame:

        # === Stage Target ===
        stage_target = self.config.get_schema_downstream(stage=stage)

        # === Table Name ===
        full_table_name = self.config.get_full_table_name(stage=stage, table_name=table_name)

        # === Partitioned By ===
        partitioned_by = self.config.get_table_partitioned_by(table_name=table_name)

        # === Query ===
        queries = self.get_formated_query(
            table_name=table_name,
            full_table_name=full_table_name,
            table_view=table_view_name,
            partitioned_by=partitioned_by
        )

        # === Write Mode ===
        write_mode = self.config.get_table_write_mode(table_name=table_name, stage=stage_target)
        if write_mode == "custom":
            dataframe.createOrReplaceTempView(table_view_name)

        # === Loader ===
        loader = self._resolve(stage, table_name, "load")

        return loader(
            stage=stage_target,
            logger=self.logger,
            session=self.session,
            config=self.config,
            table_name=table_name,
            write_mode=write_mode,
            queries=queries,
            dataframe=dataframe
        ).load()

    # =========================
    # SINGLE TABLE PIPELINE
    # =========================
    @pipeline_branch
    def run_table(self, stage: StageType, table_name: str) -> None:

        self.logger.info(f"[{stage}] Start pipeline: {table_name}")

        # === INITIALIZE ===
        table_view_name = f"{table_name}_view"

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
            dataframe=transform_stage,
            table_name=table_name,
            table_view_name=table_view_name
        )

        self.logger.info(f"[{stage}] Finished: {table_name}")

    def run_all_tables(self, stage: StageType, table_names: List[str]) -> None:
        for table_name in table_names:
            self.run_table(stage=stage, table_name=table_name)