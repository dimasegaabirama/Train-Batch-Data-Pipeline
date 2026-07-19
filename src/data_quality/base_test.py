import json
from abc import ABC

import pytest
from pydeequ.checks import Check
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import _parse_datatype_string

from src.core import (
    AppLogger,
    SchemaManager,
    Session,
    SourceManager,
    TableManager,
)


class BaseTest(ABC):

    stage = None
    table_name = None
    logger = AppLogger.get_logger()

    _table_manager = TableManager()
    _source_manager = SourceManager()
    _schema_manager = SchemaManager()

    @pytest.fixture(scope="package")
    def session(self):
        session = Session(self.logger, self.stage).get_session()
        yield session
        session.stop()

    @pytest.fixture(scope="class", autouse=True)
    def setup(self, session: SparkSession):
        self.session = session

        self.table_fullname = self._table_manager.get_table_fullname(
            self.table_name,
            self.stage,
        )

        self.dataframe = session.read.table(self.table_fullname)

    def test_schema_table(self):
        expected_schema = _parse_datatype_string(
            self._table_manager.get_table_schema(
                self.table_name,
                self.stage,
            )
        )

        expected = {
            f.name.lower(): str(f.dataType)
            for f in expected_schema.fields
        }

        actual = {
            f.name.lower(): str(f.dataType)
            for f in self.dataframe.schema.fields
        }

        assert expected == actual

    def run_tests(self, check: Check):
        check_result = (
            VerificationSuite(self.session)
            .onData(self.dataframe)
            .addCheck(check)
            .run()
        )

        result = json.loads(
            VerificationResult.checkResultsAsJson(
                self.session,
                check_result,
            )
        )

        failures = [
            constraint
            for check_result in result["checkResults"]
            for constraint in check_result["constraintResults"]
            if constraint["status"] == "Failure"
        ]

        assert not failures, failures