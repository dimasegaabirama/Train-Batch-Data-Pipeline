from __future__ import annotations

from typing import Any

from pyspark.sql.session import SparkSession
from typing_extensions import List

from src.core.config import Config
from src.models.checks_config import CheckContext
from src.models.data_config import StageType
from src.utils.table_utils import create_table_fullname


class BaseChecks:
    def __init__(
        self,
        session: SparkSession,
        catalog_name: str,
        schema_name: str,
        table_name: str,
        config: Config,
    ):
        self.session = session
        self.config = config
        self.dataframe = self.session.read.table(
            create_table_fullname(catalog_name, schema_name, table_name)
        )

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _inject_params(self, context: CheckContext, table_name: str) -> CheckContext:
        """
        Inject `table_name` into context params and remove internal flags.

        Args:
            context (CheckContext): The check context containing method and params.
            table_name (str): The table name to inject into params if key exists.

        Returns:
            CheckContext: A new CheckContext with cleaned and injected params.
        """
        params = dict(context.params or {})
        params.pop("recursive", None)
        if "table_name" in params:
            params["table_name"] = table_name
        return CheckContext(method=context.method, params=params)

    # -------------------------------------------------------------------------
    # Resolve & Run
    # -------------------------------------------------------------------------

    @classmethod
    def resolve_checks(cls, stage: StageType, table_name: str) -> List[CheckContext]:
        """
        Resolve data quality checks for a given stage and table.

        Merges default checks with table-specific overrides:
        - If table-specific checks exist, use them as the base.
        - Default checks marked with `recursive: true` are always appended.
        - If no table-specific checks exist, all default checks are used.
        - `table_name` is injected into params where the key exists.
        - `recursive` flag is stripped from the final params.

        Args:
            stage (StageType): Pipeline stage (e.g. "bronze", "silver", "gold").
            table_name (str): Target table name to resolve checks for.

        Returns:
            List[CheckContext]: Resolved list of checks to run.

        Raises:
            AttributeError: If the given stage does not exist in the config.

        Example:
            >>> checks = self.resolve_checks(stage="silver", table_name="passengers")
            >>> for check in checks:
            ...     print(check.method, check.params)
        """
        dq_base = getattr(cls.config.get_config().data_quality, stage)
        default_dq = dq_base.default or []
        table_dq = (dq_base.tables or {}).get(table_name)

        recursive_defaults = [
            cls._inject_params(c, table_name)
            for c in default_dq
            if (c.params or {}).get("recursive", False)
        ]

        if table_dq:
            return [cls._inject_params(m, table_name) for m in table_dq] + recursive_defaults
        else:
            return [cls._inject_params(c, table_name) for c in default_dq]

    def run_all(self, checks: List[CheckContext]) -> None:
        """
        Execute all resolved checks and collect errors.

        Args:
            checks (List[CheckContext]): List of check contexts to execute.

        Raises:
            ValueError: If one or more checks fail, with all errors listed.
        """
        errors = []
        for check in checks:
            method = getattr(self, check.method, None)
            if method is None:
                errors.append(f"Method '{check.method}' not found!")
                continue
            try:
                method(**(check.params or {}))
            except ValueError as e:
                errors.append(str(e))

        if errors:
            raise ValueError(
                "Data quality checks failed:\n" + "\n".join(f"- {e}" for e in errors)
            )

if __name__ == "__main__":
    pass






