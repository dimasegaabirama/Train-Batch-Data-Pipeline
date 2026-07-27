import os
from argparse import ArgumentParser
from typing_extensions import List, Dict, Tuple

from src.core import (
    AppLogger, 
    Session,
    TableManager
)

from src.app import (
    PipelineBootstrap, 
    PipelineOrchestrator
)

from src.models.data_config import DateConfig


def check_is_not_set(values: List[Tuple[str, str]]):
    for value, name in values:
        if not value:
            raise RuntimeError(f"{name} is not set !!")


def validate_dates(start_date: str, end_date: str):
    valid_date = DateConfig(
        start_date=start_date,
        end_date=end_date
    )
    return None


def validate_path(path: str, name: str):
    if not os.path.exists(path):
        raise RuntimeError(f"{name} path does not exist: {path}")
    return None


def set_env_vars(config_path: str, env_path: str, start_date: str, end_date: str):
    for key in ("CONFIG_PATH", "ENV_PATH", "START_DATE", "END_DATE"):
        os.environ.pop(key, None)

    os.environ.update({
        "CONFIG_PATH": config_path,
        "ENV_PATH": env_path,
        "START_DATE": start_date,
        "END_DATE": end_date
    })


def get_arg_or_env(args, arg, env_var):
    result = getattr(args, arg) or os.getenv(env_var)
    return result


def get_arg_or_config(args, config_manager, arg, config_key):
    value = getattr(args, arg)

    if value is None:
        value = getattr(config_manager, config_key)()

    if value is None:
        raise ValueError(f"-{arg} is required.")

    return value


def build_parser():

    parser = ArgumentParser(
        description="Pipeline runner for data transformation stages."
    )

    parser.add_argument(
        "-cfg",
        "--config",
        type=str,
        help="Path to pipeline config file"
    )

    parser.add_argument(
        "-env",
        "--environment",
        type=str,
        help="Path to environment file"
    )

    parser.add_argument(
        "-stg",
        "--stage",
        choices=["bronze", "silver", "gold"],
        required=True,
        help="Pipeline stage to run"
    )

    parser.add_argument(
        "-tbl",
        "--tables",
        nargs="+",
        type=str,
        help=(
            "List of table names to process "
            "(example: --tables users tickets routes). "
            "If not provided, all tables will be processed."
        )
    )

    parser.add_argument(
        "-start",
        "--start_date",
        type=str,
        help="Pipeline start date (YYYY-MM-DD)"
    )

    parser.add_argument(
        "-end",
        "--end_date",
        type=str,
        help="Pipeline end date (YYYY-MM-DD)"
    )

    parser.add_argument(
        "--run_bootstrap",
        action="store_true",
        help="Run Pipeline Bootstrap"
    )

    parser.add_argument(
        "--data_quality",
        action="store_true",
        help="Run Data Quality Checks"
    )

    return parser.parse_args()


def main():

    # =========================
    # Parse Arguments
    # =========================
    args = build_parser()

    stage = args.stage

    config_path = get_arg_or_env(args, "config", "CONFIG_PATH")
    env_path = get_arg_or_env(args, "environment", "ENV_PATH")
    start_date = get_arg_or_env(args, "start_date", "START_DATE")
    end_date = get_arg_or_env(args, "end_date", "END_DATE")

    # =========================
    # Initialize Dependencies
    # =========================
    logger = AppLogger("Train_Batch_Pipeline", level="INFO")

    with logger.log_context("Running Train Batch Pipeline", stage, start_date, end_date) as logger:

        with Session(stage=stage, logger=logger) as session:

            # =========================
            # Resolve Runtime Config
            # =========================
            run_bootstrap = args.run_bootstrap
            data_quality = args.data_quality

            if run_bootstrap:
                return PipelineBootstrap(session=session, logger=logger).run_bootstrap()


            check_is_not_set([
                (config_path, "CONFIG_PATH"),
                (env_path, "ENV_PATH"),
                (start_date, "START_DATE"),
                (end_date, "END_DATE")
            ])

            # =========================
            # Validation Format
            # =========================
            validate_dates(start_date, end_date)
            validate_path(config_path, "CONFIG_PATH")
            validate_path(env_path, "ENV_PATH")

            # =========================
            # Set Environment Variables
            # =========================
            set_env_vars(config_path, env_path, start_date, end_date)

            # =========================
            # Resolve Catalog and Table
            # =========================
            table_names = get_arg_or_config(args, TableManager(), "tables", "get_tablenames")

            # =========================
            # Initialize Pipeline
            # =========================
            return PipelineOrchestrator(
                logger=logger,
                session=session,
                quality_check=data_quality
            ).run_all_tables(
                stage=stage,
                table_names=table_names
            )


if __name__ == "__main__":
    main()