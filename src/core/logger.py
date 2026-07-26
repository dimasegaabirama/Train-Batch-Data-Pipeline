import logging
from contextlib import contextmanager
from time import time

from typing_extensions import Literal, Optional, Dict


class AppLogger:

    _instances: Dict[str, "AppLogger"] = {}

    def __new__(cls, name: str, *args, **kwargs) -> "AppLogger":
        if name not in cls._instances:
            cls._instances[name] = super().__new__(cls)
        return cls._instances[name]

    def __init__(
        self,
        name,
        type: Literal["file", "stream", "both"] = "stream",
        level: str = "INFO",
        log_file: Optional[str] = None,
    ):

        if hasattr(self, "_logger"):
            return

        self.name = name
        self.type = type
        self.level = level
        self.log_file = log_file
        self._logger: Optional[logging.Logger] = None
        self._build_logger()

    # -------------------------
    # Factory
    # -------------------------

    def _build_logger(self) -> logging.Logger:
        if self.type in ("file", "both") and not self.log_file:
            raise ValueError("log_file wajib diisi jika type='file' atau 'both'")

        logger = logging.getLogger(self.name)
        logger.setLevel(getattr(logging, self.level))
        logger.propagate = False
        logger.handlers.clear()

        formatter = logging.Formatter(
            "[{asctime}] | [{levelname}] | {name}:{lineno} - {message}",
            style="{",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

        if self.type in ("stream", "both"):
            stream_handler = logging.StreamHandler()
            stream_handler.setFormatter(formatter)
            logger.addHandler(stream_handler)

        if self.type in ("file", "both"):
            file_handler = logging.FileHandler(self.log_file, mode="a", encoding="utf-8")
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)

        self._logger = logger
        return self._logger

    def get_logger(self) -> logging.Logger:
        return self._logger

    # -------------------------
    # Context manager
    # -------------------------

    @contextmanager
    def log_context(self, message: str, *args):
        start_time = time()
        logger = self.get_logger()

        detail = ", ".join(map(str, args))
        header = f"{message}: {detail}" if detail else message

        try:
            logger.info(header)
            yield logger
            logger.info(f"{message}: Completed successfully")
        except Exception as e:
            logger.error(f"{message}: {e}")
            raise
        finally:
            elapsed_time = time() - start_time
            logger.info(f"{message}: Ended (Elapsed time: {elapsed_time:.2f} seconds)")