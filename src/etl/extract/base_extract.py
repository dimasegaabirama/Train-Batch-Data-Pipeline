from typing import Optional, Union, List, Dict
from abc import ABC, abstractmethod

from pyspark.sql.session import SparkSession
from pyspark.sql.column import Column
from pyspark.sql import DataFrame

from src.models.data_config import TableMetadata, TableDependency
from src.models.etl_config import ExtractResult
from src.core import SourceManager


class BaseExtract(ABC):
    """Base class for all extractors.

    Subclasses only need to implement how a single table/collection is read
    (`_read_dependency` and `_read_main_table`). The extract flow itself
    (dependencies -> main table -> build ExtractResult) lives here so it's
    defined in exactly one place.
    """

    SOURCE_TYPE: Optional[str] = None

    def __init__(
        self,
        session: SparkSession,
        main_table: TableMetadata,
        table_deps: Dict[str, List[TableDependency]],
        conditions: Optional[Dict[str, Union[str, Column]]] = None
    ):
        if main_table is None:
            raise ValueError("main_table must be provided.")

        self._source_manager = SourceManager()

        self.session: SparkSession = session
        self.conditions: Optional[Dict[str, Union[str, Column]]] = conditions

        self.main_table: TableMetadata = main_table
        self.table_name: str = self.main_table.name
        self.extract_main: bool = self.main_table.extract_main

        self.dependencies: List[TableDependency] = table_deps.get(self.table_name, [])

        self.dependency_dataframes: Dict[str, DataFrame] = {}

        self.source_config = (
            self._source_manager.get_source_config(self.SOURCE_TYPE)
            if self.SOURCE_TYPE
            else None
        )

    def extract(self) -> ExtractResult:
        """Extract dependencies, then the main table (if applicable), and build the result."""
        try:
            self._extract_dependencies()
            df = self._read_main_table() if self.extract_main else None
            return self._build_result(df)
        except Exception as e:
            raise RuntimeError(
                f"Failed to extract data for table '{self.table_name}': {e}"
            ) from e

    def _extract_dependencies(self) -> None:
        try:
            for dep in self.dependencies:
                self.dependency_dataframes[dep.name] = self._read_dependency(dep)
        except Exception as e:
            raise RuntimeError(
                f"Failed to extract dependencies for table '{self.table_name}': {e}"
            ) from e

    def _build_result(self, df: Optional[DataFrame]) -> ExtractResult:
        return ExtractResult(
            name=self.table_name,
            catalog=self.main_table.catalog,
            namespace=self.main_table.namespace,
            source_fullname=self.main_table.source_fullname,
            target_fullname=self.main_table.target_fullname,
            write_mode=self.main_table.write_mode,
            target_schema=self.main_table.target_schema,
            dataframe=df,
            queries=self.main_table.queries,
            query_params=self.main_table.query_params,
            dependencies=self.dependency_dataframes,
            extract_main=self.extract_main
        )

    @abstractmethod
    def _read_dependency(self, dep: TableDependency) -> DataFrame:
        """Read a single dependency table/collection."""

    @abstractmethod
    def _read_main_table(self) -> DataFrame:
        """Read the main table/collection for this extractor."""