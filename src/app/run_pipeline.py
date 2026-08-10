import pytest

from logging import Logger
from typing_extensions import Dict, List
from pathlib import Path

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession

from src.core import (
    AppLogger,
    DateManager,
    FilterManager,
    TableManager,
    resolve_registry_class,
)

from src.etl.extract import BaseExtract
from src.etl.transform import BaseTransform
from src.etl.load import BaseLoad

from src.models.data_config import StageType
from src.utils.nessie_utils import pipeline_branch
from src.utils.table_utils import normalize_table_info


class PipelineOrchestrator:
    def __init__(
        self, logger: Logger, session: SparkSession, quality_check: bool = False
    ):
        self.logger = logger
        self.session = session
        self.quality_check = quality_check

        self._table_manager = TableManager()
        self._date_manager = DateManager()
        self._filter_manager = FilterManager()

    def _run_tests(self, stage: StageType, table_name: str):
        try:
            self.logger.info(
                "Running Data Quality Tests for Stage: %s | Table: %s", stage, table_name
            )

            test_filename = resolve_registry_class(
                stage=stage,
                table_name=table_name,
                component_name="data_quality",
                required=False,
            )

            if test_filename is None:
                return

            dq_path = Path(__file__).parents[1] / "data_quality" / stage / test_filename

            self.logger.debug("Using Data Quality Test Class: %s", test_filename)
            self.logger.debug("Data Quality Test Path: %s", dq_path)

            return pytest.main(["-q", "--tb=short", str(dq_path)])

        except Exception as e:
            raise RuntimeError(
                f"Data Quality Tests Failed for Stage: {stage} | Error: {e}"
            )

    # =========================
    # EXTRACT
    # =========================
    def extract(self, stage: StageType, table_name: str) -> DataFrame:

        start_date = self._date_manager.get_start_date()
        end_date = self._date_manager.get_end_date()

        self.logger.info(
            "[EXTRACT] Extracting data for table: %s | Stage: %s",
            table_name,
            stage
        )

        deps = self._table_manager.get_table_deps(table_name)

        metadata_tables = [
            self._table_manager.get_table_metadata(table_name, stage), 
            *[self._table_manager.get_table_metadata(dep, stage) for dep in deps if dep["name"] != table_name]
        ]

        extractor: BaseExtract = resolve_registry_class(stage, table_name, "extract", required= False)

        condition_cls = resolve_registry_class(
            stage, table_name, "filter", required=False
        )

        field = self._filter_manager.get_field(stage, table_name)
        condition = (
            condition_cls(field=field, start_date=start_date, end_date=end_date)
            if condition_cls is not None
            else None
        )

        self.logger.debug("Using metadata_tables: %s", metadata_tables)
        self.logger.debug("Using extractor: %s", extractor)
        self.logger.debug("Using condition: %s", condition)
        self.logger.debug("Using field: %s", field)

        return extractor(
            stage=stage,
            session=self.session,
            metadata_tables=metadata_tables,
            condition=condition
        ).extract()


    # =========================
    # TRANSFORM
    # =========================
    def transform(
        self, stage: StageType, inputs: Dict[str, DataFrame], table_name: str
    ) -> DataFrame:

        self.logger.info(
            "[TRANSFORM] Transforming data for table: %s | Stage: %s", table_name, stage
        )

        transformer: BaseTransform = resolve_registry_class(
            stage=stage, table_name=table_name, component_name="transform"
        )

        self.logger.debug("Using transformer: %s", transformer)

        return transformer(
            session=self.session,
            table_name=table_name,
            inputs=inputs,
        ).transform()

    # =========================
    # LOAD
    # =========================
    def load(
        self, stage: StageType, dataframe: DataFrame, table_name: str
    ) -> DataFrame:

        self.logger.info("[LOAD] Loading data for table: %s | Stage: %s", table_name, stage)

        table_metadata = self._table_manager.get_table_metadata(table_name, stage)
        
        full_table_name = table_metadata["fullname"]
        table_view_name = f"{table_name}_view"
        partitioned_by = self._table_manager.get_table_partitioned_by(
            table_name=table_name
        )

        query_params = {
            "full_table_name": full_table_name,
            "table_view": table_view_name,
            "partitioned_by": partitioned_by,
        }

        write_mode = self._table_manager.get_table_write_mode(
            table_name=table_name, stage=stage
        )
        if write_mode == "custom":
            dataframe.createOrReplaceTempView(table_view_name)

        loader: BaseLoad = resolve_registry_class(
            stage=stage, table_name=table_name, component_name="load"
        )

        self.logger.debug("Using loader: %s", loader)
        self.logger.debug("Using query params: %s", query_params)
        self.logger.debug("Using write mode: %s", write_mode)

        return loader(
            stage=stage,
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

        extract_stage = self.extract(stage=stage, table_name=table_name)
        transform_stage = self.transform(
            stage=stage, dataframe=extract_stage, table_name=table_name
        )

        if self.quality_check:
            self._run_tests(stage=stage, table_name=table_name)

        self.load(stage=stage, dataframe=transform_stage, table_name=table_name)

    def run_all_tables(self, stage: StageType, table_names: List[str]) -> None:
        for table_name in table_names:
            self.run_table(stage=stage, table_name=table_name)
