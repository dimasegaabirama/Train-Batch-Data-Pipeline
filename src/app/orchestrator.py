from logging import Logger
from typing_extensions import Dict, List, Optional, Type

from pyspark.sql.session import SparkSession

from src.core import DateManager, FilterManager, TableManager, resolve_registry_class
from src.data_quality.dq_runner import DataQualityRunner
from src.etl.extract import BaseExtract
from src.etl.load import BaseLoad
from src.etl.transform import BaseTransform
from src.models.data_config import FilterField, StageType, TableDependency, TableMetadata
from src.models.etl_config import ExtractResult, TransformResult
from src.utils.nessie_utils import pipeline_branch
from src.utils.table_utils import create_table_view_name


class PipelineOrchestrator:
    """Coordinates the extract -> transform -> [data quality] -> load flow for a table."""

    def __init__(
        self,
        logger: Logger,
        session: SparkSession,
        quality_check: bool = False,
        custom_dq_path: Optional[str] = None,
    ):
        self.logger = logger
        self.session = session
        self.quality_check = quality_check

        self._table_manager = TableManager()
        self._date_manager = DateManager()
        self._filter_manager = FilterManager()
        self._dq_runner = DataQualityRunner(
            logger=logger, session=session, custom_dq_path=custom_dq_path
        )


    # HELPER METHODS

    def _resolve_table_metadata(self, stage: StageType, table_name: str) -> TableMetadata:
        query_params = {
            "full_table_name": self._table_manager.get_table_fullname(table_name, stage),
            "table_view": create_table_view_name(table_name)
        }
        return self._table_manager.get_table_metadata(
            table_name, stage, query_params
        )

    def _resolve_table_dependencies(self, stage: StageType, table_name: str) -> Optional[Dict[str, List[TableDependency]]]:
        return self._table_manager.get_table_deps(table_name, stage)

    def _resolve_extractor_class(self, stage: StageType, table_name: str) -> Type[BaseExtract]:
        extractor_cls: Type[BaseExtract] = resolve_registry_class(
            stage, table_name, "extract", False
        )
        if extractor_cls is None:
            raise RuntimeError(
                f"No extractor registered for Stage: {stage} | Table: {table_name}"
            )
        return extractor_cls

    def _resolve_conditions(
        self, stage: StageType, table_name: str
    ) -> Dict[str, object]:

        table_filters: List[FilterField] = self._filter_manager.get_table_filters(stage, table_name)
        type_filter = self._filter_manager.get_stage_type(stage)

        valid_filters = [f for f in table_filters if f.strategy and type_filter]

        condition_cls = {
            f.table: resolve_registry_class(f.strategy, type_filter, "filter", False)
            for f in valid_filters
        }

        if not condition_cls:
            return {}

        conditions = {}
        for f in valid_filters:
            cls = condition_cls.get(f.table)
            if cls is None:
                continue

            conditions[f.table] = cls(
                field=f.field,
                start_date=self._date_manager.get_start_date(),
                end_date=self._date_manager.get_end_date(),
                value=getattr(f, "value", None)
            )

        print(f"Resolved conditions for Stage: {stage} | Table: {table_name}: {conditions}")

        return conditions

    def _prepared_inputs(self, stage: StageType, table_name: str) -> BaseExtract:
        table_metadata = self._resolve_table_metadata(stage, table_name)
        table_deps = self._resolve_table_dependencies(stage, table_name)

        extractor_cls = self._resolve_extractor_class(stage, table_name)
        conditions = self._resolve_conditions(stage, table_name)

        self.logger.debug("Using main table: %s", table_metadata)
        self.logger.debug("Using table deps: %s", table_deps)
        self.logger.debug("Using extractor: %s", extractor_cls)
        self.logger.debug("Using conditions: %s", conditions)

        return extractor_cls(
            session=self.session,
            main_table=table_metadata,
            table_deps=table_deps,
            conditions=conditions
        )


    # EXTRACT

    def extract(self, stage: StageType, table_name: str) -> ExtractResult:
        self.logger.info(
            "[EXTRACT] Extracting data for table: %s | Stage: %s", table_name, stage
        )
        extractor = self._prepared_inputs(stage, table_name)
        return extractor.extract()


    # TRANSFORM

    def transform(
        self, stage: StageType, table_name: str, inputs: ExtractResult
    ) -> TransformResult:
        self.logger.info(
            "[TRANSFORM] Transforming data for table: %s | Stage: %s", table_name, stage
        )

        transformer_cls: Type[BaseTransform] = resolve_registry_class(
            stage, table_name, "transform"
        )

        # self.logger.debug("Dataframe before transformation: %s", inputs.dataframe.show(2))
        self.logger.debug("Using transformer: %s", transformer_cls)

        return transformer_cls(self.session, inputs).transform()


    # LOAD

    def load(self, stage: StageType, table_name: str, inputs: TransformResult):
        self.logger.info(
            "[LOAD] Loading data for table: %s | Stage: %s", table_name, stage
        )
        loader_cls: Type[BaseLoad] = resolve_registry_class(
            stage, table_name, "load"
        )

        # self.logger.debug("Dataframe after transformation: %s", inputs.cleaned_dataframe.show(2))
        self.logger.debug("Using loader: %s", loader_cls)

        return loader_cls(self.session, inputs).load()


    # SINGLE TABLE PIPELINE

    @pipeline_branch
    def run_table(self, stage: StageType, table_name: str) -> None:
        """Run extract -> transform -> (optional) DQ checks -> load for one table."""
        extract_result = self.extract(stage, table_name)

        if not extract_result.dataframe:
            self.logger.info(
                "[EXTRACT] No data found for table: %s | Stage: %s. "
                "Skipping transform, data quality, and load.",
                table_name, stage,
            )

        transform_result = self.transform(
            stage, table_name, extract_result
        )

        if self.quality_check:
            dq_passed = self._dq_runner.run(stage, table_name, transform_result)
            if not dq_passed:
                raise RuntimeError(
                    f"Aborting load: Data Quality checks did not pass for "
                    f"Stage: {stage} | Table: {table_name}"
                )

        self.load(stage, table_name, transform_result)


    # MULTI TABLE PIPELINE

    def run_all_tables(self, stage: StageType, table_names: List[str]) -> None:
        """Run the single-table pipeline sequentially for each table in the list."""
        for table_name in table_names:
            self.run_table(stage=stage, table_name=table_name)