import json
from abc import ABC
from typing_extensions import Optional

import pytest
from pydeequ.checks import Check
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql import SparkSession
from pyspark.sql.types import _parse_datatype_string

from src.core import (
    AppLogger,
    DataQualityContext,
    SchemaManager,
    Session,
    SourceManager,
    TableManager,
)


class BaseTest(ABC):
    """Base class for pydeequ-based data quality test suites.

    Subclasses set `stage` and `table_name`, then define `test_*` methods
    and/or call `run_tests` with a pydeequ `Check`.
    """

    _table_manager = TableManager()
    _source_manager = SourceManager()
    _schema_manager = SchemaManager()

    @pytest.fixture(scope="module", autouse=True)
    def setup(self):
        self.session = DataQualityContext.get_session()

        context = DataQualityContext.get_transform_result()

        self.table_name = context.name
        self.dataframe = context.cleaned_dataframe

    def test_schema_table(self):
        """Assert the dataframe's schema matches the registered table schema."""
        expected_schema = _parse_datatype_string(
            self._table_manager.get_table_schema(self.table_name, self.stage)
        )

        expected = {f.name.lower(): str(f.dataType) for f in expected_schema.fields}
        actual = {f.name.lower(): str(f.dataType) for f in self.dataframe.schema.fields}

        print(f"Expected schema: {expected}")
        print(f"Actual schema: {actual}")

        assert expected == actual

    def run_tests(self, check: Check):
        """Run a pydeequ `Check` against the dataframe and assert no constraint failed."""
        verification_result = (
            VerificationSuite(self.session).onData(self.dataframe).addCheck(check).run()
        )

        raw_result = VerificationResult.checkResultsAsJson(
            self.session, verification_result
        )
        result = (
            json.loads(raw_result)
            if isinstance(raw_result, (str, bytes, bytearray))
            else raw_result
        )

        failures = [
            constraint
            for constraint in result
            if constraint["constraint_status"] == "Failure"
        ]

        assert not failures, failures