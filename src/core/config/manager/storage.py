from src.core.config import Config
from src.models.data_config import (
    StorageContext,
    StoragesConfig,
)


class StorageManager:
    def __init__(self):
        self._config = Config.get_config()

    def get_config(self) -> StoragesConfig:
        return self._config.storages

    def get_storage_config(self, storage_name: str) -> StorageContext:
        cfg = getattr(self.get_config(), storage_name, None)
        if cfg is None:
            raise ValueError(f"Storage config for '{storage_name}' not found")
        return cfg
