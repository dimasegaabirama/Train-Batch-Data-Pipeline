from logging import Logger
from pathlib import Path

import pytest
from pyspark.sql import DataFrame, SparkSession
from typing_extensions import Optional

from src.core.dq_context import DataQualityContext
from src.core.registry import resolve_registry_class
from src.models.data_config import StageType
from src.models.etl_config import TransformResult


class DataQualityRunner:
    """Run Data Quality tests for a given stage and table."""

    def __init__(
        self, logger: Logger, session: SparkSession, custom_dq_path: Optional[str] = None
    ):
        self.logger = logger
        self.session = session
        self.custom_dq_path = custom_dq_path

    def _resolve_dq_path(self, stage: StageType, table_name: str) -> Optional[Path]:
        """Resolve which DQ test file to run: custom override takes priority over registry."""
        if self.custom_dq_path:
            self.logger.info(
                "Using CUSTOM Data Quality Test for Stage: %s | Table: %s | Path: %s",
                stage, table_name, self.custom_dq_path,
            )
            return Path(self.custom_dq_path)

        test_filename = resolve_registry_class(
            stage=stage,
            table_name=table_name,
            component_name="data_quality",
            required=False,
        )
        if test_filename is None:
            self.logger.debug(
                "No Data Quality Test registered for Stage: %s | Table: %s",
                stage, table_name,
            )
            return None

        self.logger.debug("Using Data Quality Test Class: %s", test_filename)
        return Path(__file__).parent / stage / test_filename

    def _resolve_dataframe(
        self, table_name: str, inputs: Optional[TransformResult]
    ) -> DataFrame:
        """Resolve which dataframe the DQ tests should run against."""
        if inputs is not None:
            return inputs.cleaned_dataframe
        return self.session.table(table_name)

    def _execute_dq_tests(
        self,
        stage: StageType,
        table_name: str,
        dq_path: Path,
        inputs: Optional[TransformResult],
    ) -> bool:
        """Run pytest against a resolved DQ test path and report pass/fail."""
        self.logger.info(
            "Running Data Quality Tests for Stage: %s | Table: %s | Path: %s",
            stage, table_name, dq_path,
        )

        dataframe = self._resolve_dataframe(table_name, inputs)

        DataQualityContext.set(
            session=self.session, transform_result=inputs, dataframe=dataframe
        )
        try:
            exit_code = pytest.main(["-q", "--tb=short", str(dq_path)])
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
                stage, table_name, exit_code,
            )
        return passed

    def run(
        self,
        stage: StageType,
        table_name: str,
        inputs: Optional[TransformResult] = None,
    ) -> bool:
        """Run the registered (or custom-overridden) DQ test suite for a table."""
        dq_path = self._resolve_dq_path(stage, table_name)
        if dq_path is None:
            return True

        return self._execute_dq_tests(stage, table_name, dq_path, inputs)