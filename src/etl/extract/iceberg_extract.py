from pyspark.sql import DataFrame

from src.models.data_config import TableDependency
from src.utils.table_utils import create_table_fullname

from .base_extract import BaseExtract


class IcebergExtract(BaseExtract):

    def _read_dependency(self, dep: TableDependency) -> DataFrame:
        fullname = create_table_fullname(dep.catalog, dep.namespace, dep.name)
        return self._read_table(fullname)

    def _read_main_table(self) -> DataFrame:
        return self._read_table(self.main_table.source_fullname)

    def _read_table(self, table: str) -> DataFrame:
        df = self.session.read.table(table)

        condition = self._resolve_condition(table)
        if condition is not None:
            df = df.where(condition)

        return df

    def _resolve_condition(self, table: str):
        if self.condition is not None and table == self.main_table.source_fullname:
            return self.condition
        return None