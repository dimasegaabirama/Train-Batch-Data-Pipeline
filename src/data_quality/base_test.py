from abc import ABC

from pydeequ.checks import Check
from pydeequ.verification import VerificationResult, VerificationSuite
from pyspark.sql.types import _parse_datatype_string

from src.core import (
    SchemaManager,
    Session,
    SourceManager,
    TableManager,
)


class BaseTest(ABC):
    def __init__(self, stage, table_name, logger):
        self._table_manager = TableManager()
        self._source_manager = SourceManager()
        self._schema_manager = SchemaManager()

        self.stage = stage
        self.logger = logger
        self.session = Session(self.logger, stage).get_session()

        self.table_name = table_name
        self.table_fullname = self._table_manager.get_table_fullname(
            table_name, self.stage
        )
        self.dataframe = self.session.read.table(self.table_fullname)

    def test_schema_table(self):
        expected_schema = _parse_datatype_string(
            self._table_manager.get_table_schema(self.table_name, self.stage)
        )

        expected = {f.name.lower(): str(f.dataType) for f in expected_schema.fields}

        actual = {f.name.lower(): str(f.dataType) for f in self.dataframe.schema.fields}

        assert expected == actual

    def run_tests(self, check: Check):
        check_result = (
            VerificationSuite(self.session).onData(self.dataframe).addCheck(check)
        ).run()

        result_json = VerificationResult.checkResultsAsJson(self.session, check_result)

        assert len(result_json) == 0
