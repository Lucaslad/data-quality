"""
Utilitário de Logging Estruturado para o Motor de Data Quality (Local Console + AWS CloudWatch JSON).
"""

from __future__ import annotations

import datetime as dt
import json
import logging
import os
import sys
from typing import Any


class JsonCloudWatchFormatter(logging.Formatter):
    """Formata registros de log como objetos JSON de linha única para ingestão no AWS CloudWatch Logs."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "timestamp": dt.datetime.now(dt.timezone.utc).isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        for extra_key in ("run_id", "dt_referencia", "tabela", "particao", "evento"):
            val = getattr(record, extra_key, None)
            if val is not None:
                payload[extra_key] = val
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, ensure_ascii=False)


class ConsoleCleanFormatter(logging.Formatter):
    """Formata registros de log de maneira limpa para saída no terminal local."""

    def format(self, record: logging.LogRecord) -> str:
        msg = record.getMessage()
        if record.levelno >= logging.WARNING and not msg.startswith(("[!]", "[-]", "  [")):
            return f"[{record.levelname}] {msg}"
        return msg


def configurar_logger_dq(
    nome: str = "dq_engine",
    formato: str | None = None,
    nivel: str | int = logging.INFO,
) -> logging.Logger:
    """
    Configura e retorna o logger do motor de Data Quality.
    - `formato="console"` (padrão): saída limpa e legível no terminal.
    - `formato="json"` (ou `DQ_LOG_FORMAT=json`): saída estruturada em JSON para AWS Glue / CloudWatch.
    """
    logger = logging.getLogger(nome)
    fmt_efetivo = (
        formato or os.environ.get("DQ_LOG_FORMAT", "console")
    ).strip().lower()

    if isinstance(nivel, str):
        nivel_int = getattr(logging, nivel.upper(), logging.INFO)
    else:
        nivel_int = int(nivel)

    logger.setLevel(nivel_int)
    logger.propagate = False

    # Atualiza ou cria handler de stdout
    formatter: logging.Formatter = (
        JsonCloudWatchFormatter()
        if fmt_efetivo == "json"
        else ConsoleCleanFormatter()
    )

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setFormatter(formatter)
        logger.addHandler(handler)
    else:
        for h in logger.handlers:
            h.setFormatter(formatter)

    return logger
