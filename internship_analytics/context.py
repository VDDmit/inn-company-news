from dataclasses import dataclass
from typing import Any, Optional

from .conf import DOMAIN_WEIGHTS


@dataclass
class CompanyContext:
    inn: str
    egrul_json: dict[str, Any]
    csv_json: Any
    company_full_name: str
    seo_full_name: str
    city: Optional[str]

    @property
    def domains(self) -> list[str]:
        # те же домены, что и в DOMAIN_WEIGHTS
        return list(DOMAIN_WEIGHTS.keys())
