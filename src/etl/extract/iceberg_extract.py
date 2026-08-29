from pyspark.sql import DataFrame

from src.models.data_config import TableDependency
from src.utils.table_utils import create_table_fullname

from .base_extract import BaseExtract


class IcebergExtract(BaseExtract):

    def _read_dependency(self, dep: TableDependency) -> DataFrame:
        return self._read_table(dep.catalog, dep.namespace, dep.name)

    def _read_main_table(self) -> DataFrame:
        return self._read_table(
            self.main_table.catalog,
            self.main_table.namespace,
            self.main_table.name
        )

    def _read_table(self, catalog: str, namespace: str, name: str) -> DataFrame:
        fullname = create_table_fullname(catalog, namespace, name)
        df = self.session.read.table(fullname)

        condition = self.conditions.get(name) if self.conditions else None
        if condition is not None:
            df = df.filter(condition)

        return df