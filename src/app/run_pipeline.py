from logging import Logger
from pathlib import Path
from typing_extensions import Dict, List, Type

import pytest
from pyspark.sql.session import SparkSession

from src.core import (
    DataQualityContext,
    DateManager,
    FilterManager,
    TableManager,
    resolve_registry_class,
)
from src.etl.extract import BaseExtract
from src.etl.load import BaseLoad
from src.etl.transform import BaseTransform
from src.models.data_config import StageType, TableDependency, TableMetadata
from src.models.etl_config import ExtractResult, TransformResult
from src.utils.nessie_utils import pipeline_branch


class PipelineOrchestrator:
    """Coordinates the extract -> transform -> [data quality] -> load flow for a table."""

    def __init__(
        self, logger: Logger, session: SparkSession, quality_check: bool = False
    ):
        self.logger = logger
        self.session = session
        self.quality_check = quality_check

        self._table_manager = TableManager()
        self._date_manager = DateManager()
        self._filter_manager = FilterManager()

    # =========================
    # DATA QUALITY
    # =========================
    def _run_tests(
        self, stage: StageType, table_name: str, inputs: TransformResult
    ) -> bool:
        """Run the registered data quality test suite for a table, if one exists."""
        test_filename = resolve_registry_class(
            stage=stage,
            table_name=table_name,
            component_name="data_quality",
            required=False,
        )

        if test_filename is None:
            self.logger.debug(
                "No Data Quality Test registered for Stage: %s | Table: %s",
                stage,
                table_name,
            )
            return True

        self.logger.info(
            "Running Data Quality Tests for Stage: %s | Table: %s", stage, table_name
        )

        dq_path = Path(__file__).parents[1] / "data_quality" / stage / test_filename
        self.logger.debug("Using Data Quality Test Class: %s", test_filename)
        self.logger.debug("Data Quality Test Path: %s", dq_path)

        DataQualityContext.set(transform_result=inputs)
        try:
            exit_code = pytest.main(["-q", "-v", str(dq_path)])
        except Exception as e:
            raise RuntimeError(
                f"Data Quality Tests Failed for Stage: {stage} | Error: {e}"
            ) from e
        finally:
            DataQualityContext.clear()

        passed = exit_code == 0
        if not passed:
            self.logger.error(
                "Data Quality Tests FAILED for Stage: %s | Table: %s | Exit code: %s",
                stage,
                table_name,
                exit_code,
            )

        return passed

    # =========================
    # SHARED SETUP
    # =========================
    def _prepared_inputs(self, stage: StageType, table_name: str) -> BaseExtract:
        """Build a ready-to-run extractor instance for the given stage/table."""
        query_params = {
            "full_table_name": self._table_manager.get_table_fullname(table_name, stage),
            "table_view": f"{table_name}_view",
        }

        table_metadata: TableMetadata = self._table_manager.get_table_metadata(
            table_name, stage, query_params
        )
        table_deps: Dict[str, List[TableDependency]] = self._table_manager.get_table_deps(
            table_name, stage
        )

        extractor_cls: Type[BaseExtract] = resolve_registry_class(
            stage=stage, table_name=table_name, component_name="extract", required=False
        )
        if extractor_cls is None:
            raise RuntimeError(
                f"No extractor registered for Stage: {stage} | Table: {table_name}"
            )

        field = self._filter_manager.get_field(stage, table_name)
        condition_cls = resolve_registry_class(
            stage=stage, table_name=table_name, component_name="filter", required=False
        )
        condition = (
            condition_cls(
                field=field,
                start_date=self._date_manager.get_start_date(),
                end_date=self._date_manager.get_end_date(),
            )
            if condition_cls is not None
            else None
        )

        self.logger.debug("Using main table: %s", table_metadata)
        self.logger.debug("Using table deps: %s", table_deps)
        self.logger.debug("Using extractor: %s", extractor_cls)
        self.logger.debug("Using condition: %s", condition)
        self.logger.debug("Using field: %s", field)

        return extractor_cls(
            session=self.session,
            main_table=table_metadata,
            table_deps=table_deps,
            condition=condition,
        )

    # =========================
    # EXTRACT
    # =========================
    def extract(self, stage: StageType, table_name: str) -> ExtractResult:
        self.logger.info(
            "[EXTRACT] Extracting data for table: %s | Stage: %s", table_name, stage
        )

        extractor = self._prepared_inputs(stage, table_name)
        return extractor.extract()

    # =========================
    # TRANSFORM
    # =========================
    def transform(
        self, stage: StageType, table_name: str, inputs: ExtractResult
    ) -> TransformResult:
        self.logger.info(
            "[TRANSFORM] Transforming data for table: %s | Stage: %s", table_name, stage
        )

        transformer_cls: Type[BaseTransform] = resolve_registry_class(
            stage=stage, table_name=table_name, component_name="transform"
        )
        self.logger.debug("Using transformer: %s", transformer_cls)

        return transformer_cls(
            stage=stage,
            session=self.session,
            extract_result=inputs,
        ).transform()

    # =========================
    # LOAD
    # =========================
    def load(self, stage: StageType, table_name: str, inputs: TransformResult):
        self.logger.info(
            "[LOAD] Loading data for table: %s | Stage: %s", table_name, stage
        )

        loader_cls: Type[BaseLoad] = resolve_registry_class(
            stage=stage, table_name=table_name, component_name="load"
        )
        self.logger.debug("Using loader: %s", loader_cls)

        return loader_cls(
            session=self.session,
            transform_result=inputs,
        ).load()

    # =========================
    # SINGLE TABLE PIPELINE
    # =========================
    @pipeline_branch
    def run_table(self, stage: StageType, table_name: str) -> None:
        """Run extract -> transform -> (optional) DQ checks -> load for one table."""
        extract_result = self.extract(stage, table_name)
        transform_result = self.transform(
            stage=stage, table_name=table_name, inputs=extract_result
        )

        if self.quality_check:
            dq_passed = self._run_tests(stage, table_name, transform_result)
            if not dq_passed:
                raise RuntimeError(
                    f"Aborting load: Data Quality checks did not pass for "
                    f"Stage: {stage} | Table: {table_name}"
                )

        self.load(stage=stage, table_name=table_name, inputs=transform_result)

    # =========================
    # MULTI TABLE PIPELINE
    # =========================
    def run_all_tables(self, stage: StageType, table_names: List[str]) -> None:
        """Run the single-table pipeline sequentially for each table in the list."""
        for table_name in table_names:
            self.run_table(stage=stage, table_name=table_name)