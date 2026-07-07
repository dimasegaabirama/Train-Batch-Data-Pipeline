import logging
from functools import wraps
from time import time

from typing_extensions import Literal, Optional

from src.models.data_config import StageType


class AppLogger:
    """
    Singleton logger factory.

    Usage:
        logger = AppLogger.get_logger(name="pipeline", type="stream")
    """

    _instance: "AppLogger | None" = None
    _logger: "logging.Logger | None" = None

    # -------------------------
    # Singleton
    # -------------------------

    def __new__(cls, *args, **kwargs) -> "AppLogger":
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    # -------------------------
    # Factory
    # -------------------------

    @classmethod
    def get_logger(
        cls,
        name: str = __name__,
        type: Literal["file", "stream", "both"] = "stream",
        log_file: Optional[str] = None,
        force_update: bool = False,
    ) -> logging.Logger:
        """
        Return (and cache) a configured logger instance.

        Parameters
        ----------
        name         : logger name, defaults to module name
        type         : output target — 'stream', 'file', or 'both'
        log_file     : required when type is 'file' or 'both'
        force_update : rebuild the logger even if one already exists
        """
        if cls._logger is not None and not force_update:
            return cls._logger

        if type in ("file", "both") and not log_file:
            raise ValueError("log_file wajib diisi jika type='file' atau 'both'")

        logger = logging.getLogger(name)
        logger.setLevel(logging.INFO)
        logger.propagate = False
        logger.handlers.clear()

        formatter = logging.Formatter(
            "[{asctime}] | [{levelname}] | {name}:{lineno} - {message}",
            style="{",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        if type in ("stream", "both"):
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)

        if type in ("file", "both"):
            file_handler = logging.FileHandler(log_file, mode="a", encoding="utf-8")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        cls._logger = logger
        return cls._logger

    # -------------------------
    # Decorator
    # -------------------------

    @staticmethod
    def log_stage(stage: "StageType | None" = None):
        def decorator(func):
            @wraps(func)
            def wrapper(*args, **kwargs):
                logger: Optional[logging.Logger] = kwargs.get("logger") or (
                    getattr(args[0], "logger", None) if args else None
                )

                label = stage or func.__name__

                try:
                    start = time()
                    if logger:
                        logger.info(f"Start stage: {label}")

                    result = func(*args, **kwargs)

                    if logger:
                        logger.info(f"End stage: {label}, time: {time() - start:.2f}s")
                    return result

                except Exception as e:
                    if logger:
                        logger.exception(f"Error in stage '{label}': {e}")
                    raise

            return wrapper

        return decorator
