from pydantic import BaseModel
from typing_extensions import Dict, List, Optional

class CheckContext(BaseModel):
    method: str
    params: Optional[Dict[str, object]] = None  # Any karena value bisa str, list, dll

class CheckConfig(BaseModel):
    checks: List[CheckContext]

class QualityContext(BaseModel):
    default: Optional[List[CheckContext]] = None        # list of checks
    tables: Optional[Dict[str, CheckConfig]] = None  # per-table override

class QualityConfig(BaseModel):
    bronze: Optional[QualityContext] = None
    silver: Optional[QualityContext] = None
    gold:   Optional[QualityContext] = None