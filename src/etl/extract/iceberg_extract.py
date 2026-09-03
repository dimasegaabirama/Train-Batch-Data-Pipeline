from pyspark.sql import DataFrame

from src.models.data_config import TableDependency
from src.utils.table_utils import create_table_fullname
from src.utils.text_utils import split_text

from .base_extract import BaseExtract


class IcebergExtract(BaseExtract):

    def _read_dependency(self, dep: TableDependency) -> DataFrame:
        fullname = create_table_fullname(dep.catalog, dep.namespace, dep.name)
        return self._read_table(fullname)

    def _read_main_table(self) -> DataFrame:
        return self._read_table(self.main_table.source_fullname)

    def _read_table(self, fullname: str) -> DataFrame:
        catalog, namespace, name = split_text(fullname, ".")
        df = self.session.read.table(fullname)

        condition = self.conditions.get(name) if self.conditions else None
        if condition is not None:
            df = df.filter(condition)

        return df