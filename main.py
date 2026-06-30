import os
from argparse import ArgumentParser

from src.core.logger import AppLogger
from src.core.config import Config
from src.core.session import Session

from src.app.run_pipeline import PipelineOrchestrator
from src.models.data_config import DateConfig


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
            "(example: --tables users tickets routes)"
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

    return parser.parse_args()


def main():

    # =========================
    # Parse Arguments
    # =========================
    args = build_parser()

    stage = args.stage

    # =========================
    # Resolve Runtime Config
    # =========================
    config_path = args.config or os.getenv("CONFIG_PATH")
    env_path = args.environment or os.getenv("ENV_PATH")
    start_date = args.start_date or os.getenv("START_DATE")
    end_date = args.end_date or os.getenv("END_DATE")

    if not config_path:
        raise EnvironmentError("CONFIG_PATH is not set.")

    if not env_path:
        raise EnvironmentError("ENV_PATH is not set.")

    if not start_date:
        raise EnvironmentError("START_DATE is not set.")

    if not end_date:
        raise EnvironmentError("END_DATE is not set.")

    os.environ.update({
        "CONFIG_PATH": config_path,
        "ENV_PATH": env_path,
        "START_DATE": start_date,
        "END_DATE": end_date
    })

    # =========================
    # Date Validation
    # =========================
    DateConfig(
        start_date=start_date,
        end_date=end_date
    )

    # =========================
    # Initialize Dependencies
    # =========================
    config = Config()

    logger = AppLogger.get_logger()

    session = Session(
        logger=logger,
        config=config
    ).get_session(stage=stage)

    # =========================
    # Resolve Catalog and Table
    # =========================
    pipeline_cfg = config.get_pipeline_config()
    table_names = args.tables or pipeline_cfg.tablenames
    
    print(args.tables)
    print(pipeline_cfg.tablenames)

    if not table_names:
        raise ValueError(
            "--tables is required."
        )

    # =========================
    # Initialize Pipeline
    # =========================
    pipeline = PipelineOrchestrator(
        logger=logger,
        spark=session,
        config=config
    )

    # =========================
    # Execute Pipeline
    # =========================
    pipeline.run_all_tables(
        stage=stage,
        table_names=table_names
    )


if __name__ == "__main__":
    main()