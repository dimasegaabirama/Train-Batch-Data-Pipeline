import pytest
from pydeequ.checks import Check, CheckLevel
from pydeequ.verification import VerificationResult, VerificationSuite

from src.core import DATE_COLUMNS
from src.data_quality.base_test import BaseTest


class TestBronze(BaseTest):

    stage = "bronze"

    def test_completeness(self):
        check = Check(self.session, CheckLevel.Warning).isComplete(
            "id", "ID Shouldn't have null value !!"
        )

        self.run_tests(check)