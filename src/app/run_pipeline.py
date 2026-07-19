from typing import List

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from src.core import (
    AppLogger,
    DateManager,
    FilterManager,
    TableManager,
    resolve_registry_class,
)
from src.etl.transform import BaseTransform
from src.models.data_config import StageType
from src.utils.nessie_utils import pipeline_branch


class PipelineOrchestrator:
    def __init__(self, logger: AppLogger, session: SparkSession):
        self.logger = logger
        self.session = session

        self._table_manager = TableManager()
        self._date_manager = DateManager()
        self._filter_manager = FilterManager()

    # =========================
    # EXTRACT
    # =========================
    def extract(self, stage: StageType, table_name: str) -> DataFrame:

        start_date = self._date_manager.get_start_date()
        end_date = self._date_manager.get_end_date()

        # === Extractor ===
        extractor = resolve_registry_class(stage, table_name, "extract")
        self.logger.debug(f"Extractor: {extractor}")

        # === Condition (filter) ===
        condition_cls = resolve_registry_class(
            stage, table_name, "filter", required=False
        )
        self.logger.debug(f"Condition Class: {condition_cls}")

        field = self._filter_manager.get_field(stage, table_name)
        self.logger.debug(f"Field: {field}")


        condition = (
            condition_cls(field=field, start_date=start_date, end_date=end_date)
            if condition_cls is not None
            else None
        )
        self.logger.debug(f"Condition: {condition}")

        return extractor(
            stage=stage,
            logger=self.logger,
            session=self.session,
            table_name=table_name,
            condition=condition
        ).extract()

    # =========================
    # TRANSFORM
    # =========================
    def transform(
        self, stage: StageType, dataframe: DataFrame, table_name: str
    ) -> DataFrame:

        lookup_tables = self._table_manager.get_table_deps(table_name)
        self.logger.debug(f"Lookup Tables : {lookup_tables}")

        # === Transformer ===
        transformer: BaseTransform = resolve_registry_class(
            stage=stage, table_name=table_name, component_name="transform"
        )
        self.logger.debug(f"Transformer : {transformer}")

        return transformer(
            logger=self.logger,
            session=self.session,
            dataframe=dataframe,
            lookup_tables=lookup_tables,
        ).transform()

    # =========================
    # LOAD
    # =========================
    def load(
        self, stage: StageType, dataframe: DataFrame, table_name: str
    ) -> DataFrame:

        # === Table Name ===
        full_table_name = self._table_manager.get_table_fullname(
            stage=stage, table_name=table_name
        )
        table_view_name = f"{table_name}_view"

        # === Partitioned By ===
        partitioned_by = self._table_manager.get_table_partitioned_by(
            table_name=table_name
        )

        # === Query Params ===
        query_params = {
            "full_table_name": full_table_name,
            "table_view": table_view_name,
            "partitioned_by": partitioned_by
        }
        self.logger.debug(f"query_params :, {query_params}")

        # === Write Mode ===
        write_mode = self._table_manager.get_table_write_mode(
            table_name=table_name, stage=stage
        )
        self.logger.debug(f"write_mode :, {write_mode}")
        if write_mode == "custom":
            self.logger.debug(f"Write Mode : Custom, Create Temp Table View: {table_view_name}!!")
            dataframe.createOrReplaceTempView(table_view_name)

        # === Loader ===
        loader = resolve_registry_class(
            stage=stage, table_name=table_name, component_name="load"
        )
        self.logger.debug(f"Loader: {loader}")

        return loader(
            stage=stage,
            logger=self.logger,
            session=self.session,
            dataframe=dataframe,
            table_name=table_name,
            query_params=query_params,
        ).load()

    # =========================
    # SINGLE TABLE PIPELINE
    # =========================
    @pipeline_branch
    def run_table(self, stage: StageType, table_name: str) -> None:

        self.logger.info(f"[{stage}] Start pipeline: {table_name}")

        # === EXTRACT ===
        self.logger.info(f"[{stage}] Extract Table: {table_name}")
        extract_stage = self.extract(stage=stage, table_name=table_name)
        self.logger.debug(f"Extract Table {table_name}: {extract_stage.show()}")

        # === TRANSFORM ===
        self.logger.info(f"[{stage}] Transform Table: {table_name}")
        transform_stage = self.transform(
            stage=stage, dataframe=extract_stage, table_name=table_name
        )
        self.logger.debug(f"Transform Table {table_name}: {transform_stage.show()}")

        # === LOAD ===
        self.logger.info(f"[{stage}] Load Table: {table_name}")
        self.load(stage=stage, dataframe=transform_stage, table_name=table_name)

        self.logger.info(f"[{stage}] Finished: {table_name}")

    def run_all_tables(self, stage: StageType, table_names: List[str]) -> None:
        for table_name in table_names:
            self.run_table(stage=stage, table_name=table_name)
