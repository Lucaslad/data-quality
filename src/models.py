"""Modelos de dados padronizados para os resultados do motor de Data Quality."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any


@dataclass
class ResultadoCheck:
    """Representa o resultado individual de uma validação de Data Quality."""

    run_id: str
    dt_referencia: str
    ts_execucao: str
    tabela: str
    particao: str
    nivel_check: str  # PARTICAO | COLUNA | MATURACAO_EVENTO | FOTOGRAFIA_DIFF
    tipo_check: str
    coluna: str
    valor_observado: float | str | None
    valor_esperado: str
    status: str  # OK | WARNING | ANOMALIA
    detalhes: str

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        if d["valor_observado"] is not None:
            d["valor_observado"] = str(d["valor_observado"])
        return d
