from typing import Optional
from src.models.etl_config import TransformResult


class DataQualityContext:
    """
    Shared in-process context untuk oper TransformResult dari
    PipelineOrchestrator ke pytest BaseTest, tanpa lewat disk/SQL view.
    Aman karena pytest.main() dijalankan in-process (sama interpreter,
    sama SparkContext).
    """
    _current: Optional[TransformResult] = None

    @classmethod
    def set(cls, transform_result: TransformResult) -> None:
        cls._current = transform_result

    @classmethod
    def get(cls) -> Optional[TransformResult]:
        return cls._current

    @classmethod
    def clear(cls) -> None:
        cls._current = None