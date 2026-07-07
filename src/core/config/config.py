import os

import yaml
from dotenv import load_dotenv

from src.models.base_config import BaseConfig
from src.utils.text_utils import replace_env


class Config:
    """
    Singleton configuration manager.

    Loads configuration from YAML and environment variables once,
    then exposes helper methods for accessing specific config sections.
    """

    _instance: "Config | None" = None
    _config: "BaseConfig | None" = None

    # =========================
    # Singleton
    # =========================

    def __new__(cls, *args, **kwargs) -> "Config":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    # =========================
    # Core Loader
    # =========================

    @classmethod
    def get_config(cls) -> BaseConfig:
        """Load and cache configuration from YAML + env vars."""
        if cls._config is None:
            cls._config = cls._load_config()
        return cls._config

    @classmethod
    def _load_config(cls) -> BaseConfig:
        load_dotenv(".env.global")
        load_dotenv(os.getenv("ENV_PATH"))

        config_path = cls._require(
            os.getenv("CONFIG_PATH"),
            "CONFIG_PATH environment variable is not set",
        )

        with open(config_path) as f:
            raw = yaml.safe_load(f)

        return BaseConfig(**replace_env(raw))

if __name__ == "__main__":
    print(Config.get_query_table("passengers"))
