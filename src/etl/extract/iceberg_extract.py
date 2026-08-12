from typing_extensions import Dict, List, Optional

from src.models.etl_config import ExtractResult
from src.utils.table_utils import create_table_fullname

from .base_extract import BaseExtract


class IcebergExtract(BaseExtract):

    def extract(self, extract_main: Optional[bool] = True) -> ExtractResult:

        full_table_name = self.main_table.fullname

        deps_table: Dict[str, "DataFrame"] = {}

        try:
            dependencies = self.table_deps[self.table_name]
            for dep in dependencies:
                deps_table[dep.name] = self._read_table(
                    create_table_fullname(dep.catalog, dep.schema_name, dep.name)
                )

            if not extract_main and not deps_table:
                raise ValueError(
                    f"Cannot extract table '{self.table_name}': extract_main is set to False, "
                    f"but no dependencies are defined for this table."
                )
        
            df = self._read_table(full_table_name) if extract_main else None

            return ExtractResult(
                name=self.table_name,
                catalog=self.main_table.catalog,
                schema_name=self.main_table.schema_name,
                fullname=full_table_name,
                location=self.main_table.location,
                write_mode=self.main_table.write_mode,
                dataframe=df,
                queries=self.main_table.queries,
                dependencies=deps_table
            )
        
        except Exception as e:
            raise RuntimeError(f"Failed to extract data for table '{self.table_name}': {e}") from e

    def _read_table(self, table: str) -> "DataFrame":
        df = self.session.read.table(table)

        condition = self._resolve_condition(table)
        if condition is not None:
            df = df.where(condition)

        return df

    def _resolve_condition(self, table: str):
        if self.condition is not None and table == self.main_table.fullname:
            return self.condition
        return None
