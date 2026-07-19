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

    return parser.parse_args()


def main():

    # =========================
    # Parse Arguments
    # =========================
    args = build_parser()

    stage = args.stage

    config_path = args.config or os.getenv("CONFIG_PATH")
    env_path = args.environment or os.getenv("ENV_PATH")
    start_date = args.start_date or os.getenv("START_DATE")
    end_date = args.end_date or os.getenv("END_DATE")

    check_is_not_set([
        (config_path, "CONFIG_PATH"),
        (env_path, "ENV_PATH"),
        (start_date, "START_DATE"),
        (end_date, "END_DATE")
    ])

    # =========================
    # Date Validation
    # =========================

    DateConfig(
        start_date=start_date,
        end_date=end_date
    )

    for key in ("CONFIG_PATH", "ENV_PATH", "START_DATE", "END_DATE"):
        os.environ.pop(key, None)

    os.environ.update({
        "CONFIG_PATH": config_path,
        "ENV_PATH": env_path,
        "START_DATE": start_date,
        "END_DATE": end_date
    })

    # =========================
    # Resolve Catalog and Table
    # =========================
    pipeline_cfg = TableManager()
    table_names = args.tables or pipeline_cfg.get_tablenames()

    if not table_names:
        raise ValueError(
            "--tables is required."
        )

    # =========================
    # Initialize Dependencies
    # =========================
    logger = AppLogger.get_logger(level="DEBUG")

    session = Session(
        logger=logger,
        stage=stage
    ).get_session()

    # =========================
    # Resolve Runtime Config
    # =========================
    run_bootstrap = args.run_bootstrap
    if run_bootstrap:
        return PipelineBootstrap(session=session, logger=logger).run_bootstrap()

    # =========================
    # Initialize Pipeline
    # =========================
    return PipelineOrchestrator(
        logger=logger,
        session=session
    ).run_all_tables(
        stage=stage,
        table_names=table_names
    )


if __name__ == "__main__":
    main()