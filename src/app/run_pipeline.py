import os
from argparse import ArgumentParser
from typing_extensions import List, Tuple, Dict, Optional

from src.app.orchestrator import PipelineOrchestrator
from src.app.run_bootstrap import PipelineBootstrap
from src.core import AppLogger, Session, TableManager
from src.models.data_config import DateConfig


class PipelineRunner:
    """
    Encapsulates argument parsing, validation, environment setup,
    and execution of the data pipeline (bronze/silver/gold stages).
    """

    def __init__(self):
        self.args = None

        self.stage: Optional[str] = None
        self.config_path: Optional[str] = None
        self.env_path: Optional[str] = None
        self.start_date: Optional[str] = None
        self.end_date: Optional[str] = None
        self.run_bootstrap: bool = False
        self.data_quality: bool = False
        self.dq_path: Optional[str] = None
        self.table_names: Optional[List[str]] = None

        self.required_config: bool = True
        self.required_env: bool = True
        self.required_date: bool = True
        self.required_table_names: bool = True

    # =========================
    # Argument Parsing
    # =========================
    @staticmethod
    def build_parser():
        parser = ArgumentParser(
            description="Pipeline runner for data transformation stages."
        )

        parser.add_argument("-cfg", "--config", type=str, help="Path to pipeline config file")
        parser.add_argument("-env", "--environment", type=str, help="Path to environment file")
        parser.add_argument(
            "-stg", "--stage",
            choices=["bootstrap", "bronze", "silver", "gold"],
            help="Pipeline stage to run",
        )
        parser.add_argument(
            "-tbl", "--tables", nargs="+", type=str,
            help=(
                "List of table names to process "
                "(example: --tables users tickets routes). "
                "If not provided, all tables will be processed."
            ),
        )
        parser.add_argument("-start", "--start_date", type=str, help="Pipeline start date (YYYY-MM-DD)")
        parser.add_argument("-end", "--end_date", type=str, help="Pipeline end date (YYYY-MM-DD)")
        parser.add_argument("--run_bootstrap", action="store_true", help="Run Pipeline Bootstrap")
        parser.add_argument("--data_quality", action="store_true", help="Run Data Quality Checks")
        parser.add_argument(
            "-dqp", "--dq_path", type=str,
            help=(
                "Path to a custom Data Quality test file/module. "
                "Overrides the auto-resolved DQ test from the registry. "
                "Only used if --data_quality is also set."
            ),
        )

        return parser.parse_args()

    def parse_args(self):
        self.args = self.build_parser()
        return self.args

    # =========================
    # Helpers
    # =========================
    def get_arg_or_env(self, arg: str, env_var: str):
        return getattr(self.args, arg) or os.getenv(env_var)

    @staticmethod
    def get_arg_or_config(args, config_manager, arg, config_key, config_params=None, required=True):
        value = getattr(args, arg)
        if value is None:
            value = getattr(config_manager, config_key)(**(config_params or {}))
        if value is None and required:
            raise ValueError(f"-{arg} is required.")
        return value

    @staticmethod
    def validate_dates(start_date: str, end_date: str):
        DateConfig(start_date=start_date, end_date=end_date)
        return True

    @staticmethod
    def validate_path(path: str, name: str):
        if not os.path.exists(path):
            raise RuntimeError(f"{name} path does not exist: {path}")
        return True

    @staticmethod
    def set_env_vars(values: List[Tuple[str, str, bool]]):
        for key, value, required in values:
            if not value and required:
                raise RuntimeError(f"{key} is not set !!")
            if value:
                os.environ.pop(key, None)
                os.environ[key] = value

    # =========================
    # Resolution Steps
    # =========================
    def resolve_params(self):
        self.stage = self.args.stage
        self.config_path = self.get_arg_or_env("config", "CONFIG_PATH")
        self.env_path = self.get_arg_or_env("environment", "ENV_PATH")
        self.start_date = self.get_arg_or_env("start_date", "START_DATE")
        self.end_date = self.get_arg_or_env("end_date", "END_DATE")
        self.run_bootstrap = self.args.run_bootstrap

        if self.run_bootstrap:
            self.stage = "bootstrap"
            self.required_date = False
            self.required_table_names = False

        self.data_quality = self.args.data_quality
        self.dq_path = self.args.dq_path

    def validate(self):
        if self.required_date:
            self.validate_dates(self.start_date, self.end_date)
        if self.required_config:
            self.validate_path(self.config_path, "CONFIG_PATH")
        if self.required_env:
            self.validate_path(self.env_path, "ENV_PATH")
        if self.dq_path:
            self.validate_path(self.dq_path, "DQ_PATH")
            if not self.data_quality:
                raise ValueError("--dq_path requires --data_quality to also be set.")
        if not self.run_bootstrap:
            if self.stage not in ["bronze", "silver", "gold"]:
                raise ValueError(
                    f"Invalid stage '{self.stage}'. Must be one of: bronze, silver, gold."
                )

    def apply_env_vars(self):
        self.set_env_vars([
            ("CONFIG_PATH", self.config_path, self.required_config),
            ("ENV_PATH", self.env_path, self.required_env),
            ("START_DATE", self.start_date, self.required_date),
            ("END_DATE", self.end_date, self.required_date),
        ])

    def resolve_tables(self):
        if not self.run_bootstrap:
            self.table_names = self.get_arg_or_config(
                args=self.args,
                config_manager=TableManager(),
                arg="tables",
                config_key="get_tablenames",
                config_params={"stage": self.stage},
                required=self.required_table_names,
            )
        return self.table_names

    # =========================
    # Execution
    # =========================
    def run(self):
        self.parse_args()
        self.resolve_params()
        self.validate()
        self.apply_env_vars()
        self.resolve_tables()

        logger = AppLogger("Train_Batch_Pipeline", level="INFO")

        with logger.log_context(
            "Running Train Batch Pipeline", self.stage, self.start_date, self.end_date
        ) as logger:
            with Session(stage=self.stage, logger=logger) as session:
                if self.run_bootstrap:
                    return PipelineBootstrap(session=session, logger=logger).run_bootstrap()

                return PipelineOrchestrator(
                    logger=logger,
                    session=session,
                    quality_check=self.data_quality,
                    custom_dq_path=self.dq_path,
                ).run_all_tables(stage=self.stage, table_names=self.table_names)


def main():
    return PipelineRunner().run()


if __name__ == "__main__":
    main()