from typing_extensions import Dict
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.session import SparkSession
from typing_extensions import List

from src.core.config import Config
from src.core.logger import AppLogger
from src.core.registry import (
    _EXTRACT_REGISTRY,
    _FILTER_REGISTRY,
    _LOAD_REGISTRY,
    _TRANSFORMER_REGISTRY,
    resolve_registry_class,
)
from src.models.data_config import StageType
from src.models.pipeline_config import FlowKey
from src.utils.nessie_utils import pipeline_branch


class PipelineOrchestrator:
    def __init__(
        self,
        logger: AppLogger,
        spark: SparkSession,
        config: Config
    ):
        self.logger = logger
        self.spark = spark
        self.config = config

    # =========================
    # FIND DEPENDENCY
    # =========================
    def get_table_deps(self, table_names: List[str]) -> Dict[str, str]:

        dependencies = {}

        for table_name in table_names:
            cfg = getattr(self.config.get_table_config(table_name), "depends_on", None)

            if not cfg:
                continue

            dependencies.update({
                key: f"{val['catalog']}.{val['schema']}.{key}" for key, val in cfg.items()
            })
        
        return dependencies

    # =========================
    # PIPELINE FLOW
    # =========================
    def get_pipeline_flow(self, stage: StageType) -> Dict[FlowKey, StageType]:
        cfg = getattr(self.config.get_pipeline_config(), "schema_flow", None)

        if cfg is None:
            raise ValueError("Schema flow from Pipeline Config not found!")

        flow_cfg = getattr(cfg, stage, None)

        if flow_cfg is None:
            raise ValueError(f"Pipeline flow for {stage} not found!")

        return flow_cfg

    # =========================
    # EXTRACT
    # =========================
    def extract(self, stage: StageType, table_name: str) -> DataFrame:

        catalog_type = self.config.get_catalog_type()

        start_date = self.config.get_start_date()
        end_date = self.config.get_end_date()

        # === Extractor ===
        extractor = resolve_registry_class(
            registry=_EXTRACT_REGISTRY,
            stage=stage,
            table_name=table_name,
            component_name="Extractor",
        )

        # === Condition ===
        condition = resolve_registry_class(
            registry=_FILTER_REGISTRY,
            stage=stage,
            table_name=table_name,
            component_name="Filter",
            required=False,
        )

        field = self.config.get_filter_field(stage=stage, table_name=table_name)
        if field:
            condition = condition(
                stage=stage, field=field, start_date=start_date, end_date=end_date
            )

        return extractor(
            logger=self.logger,
            session=self.spark,
            config=self.config,
            catalog_type=catalog_type,
            table_name=table_name,
            stage=stage,
            condition=condition,
        ).extract()

    # =========================
    # TRANSFORM
    # =========================
    def transform(
        self, stage: StageType, dataframe: DataFrame, table_name: str
    ) -> DataFrame:

        lookup_table_names = self.get_table_deps(table_name)

        # === Transformer ===
        transformer = resolve_registry_class(
            registry=_TRANSFORMER_REGISTRY,
            stage=stage,
            table_name=table_name,
            component_name="Transform",
        )

        return transformer(
            session=self.spark,
            config=self.config,
            dataframe=dataframe,
            lookup_table_name=lookup_table_names,
        ).transform()

    # =========================
    # LOAD
    # =========================
    def load(
        self,
        stage: StageType,
        stage_target: StageType,
        dataframe: DataFrame,
        table_name: str,
    ) -> DataFrame:
        # === Loader ===
        loader = resolve_registry_class(
            registry=_LOAD_REGISTRY,
            stage=stage,
            table_name=table_name,
            component_name="Load",
        )

        return loader(
            logger=self.logger,
            session=self.spark,
            config=self.config,
            table_name=table_name,
            stage=stage_target,
            dataframe=dataframe
        ).load()

    # =========================
    # SINGLE TABLE PIPELINE
    # =========================
    @pipeline_branch
    def run_table(self, stage: StageType, table_name: str) -> None:

        self.logger.info(f"[{stage}] Start pipeline: {table_name}")

        # GET STAGE TARGET
        stage_target = self.get_pipeline_flow(stage=stage)["target"]

        # EXTRACT, TRANSFORM, LOAD
        print("=== extract ===")
        extract_stage = self.extract(stage=stage, table_name=table_name)
        print(extract_stage.show())

        print("=== transform ===")
        transform_stage = self.transform(
            stage=stage, dataframe=extract_stage, table_name=table_name
        )
        print(transform_stage.show())

        print("=== load ===")
        self.load(
            stage=stage,
            stage_target=stage_target,
            dataframe=transform_stage,
            table_name=table_name
        )

        self.logger.info(f"[{stage}] Finished: {table_name} ✅\n")

    def run_all_tables(self, stage: str, table_names: List[str]) -> None:
        for table_name in table_names:
            self.run_table(stage=stage, table_name=table_name)


if __name__ == "__main__":
    pass
