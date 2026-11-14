"""Carregador de configuração YAML para o motor de Data Quality."""

from __future__ import annotations

import ast
from pathlib import Path
from typing import Any


def _parse_scalar(val: str) -> Any:
    """Converte um valor escalar YAML em tipo nativo Python."""
    val = val.strip()
    if not val:
        return None
    if val in ("true", "True"):
        return True
    if val in ("false", "False"):
        return False
    if val in ("null", "None", "~"):
        return None
    if val == "{}":
        return {}
    if val == "[]":
        return []
    if (val.startswith("[") and val.endswith("]")) or (
        val.startswith("{") and val.endswith("}")
    ):
        try:
            return ast.literal_eval(val)
        except Exception:
            pass
    if (val.startswith('"') and val.endswith('"')) or (
        val.startswith("'") and val.endswith("'")
    ):
        try:
            return ast.literal_eval(val)
        except Exception:
            return val[1:-1]
    try:
        if "." in val:
            return float(val)
        return int(val)
    except ValueError:
        return val


def _strip_inline_comment(line: str) -> str:
    """Remove comentários inline preservando conteúdo entre aspas."""
    in_single = False
    in_double = False
    escaped = False
    for idx, ch in enumerate(line):
        if escaped:
            escaped = False
            continue
        if ch == "\\":
            escaped = True
            continue
        if ch == "'" and not in_double:
            in_single = not in_single
        elif ch == '"' and not in_single:
            in_double = not in_double
        elif ch == "#" and not in_single and not in_double:
            return line[:idx]
    return line


def _fallback_yaml_load(text: str) -> dict[str, Any]:
    """
    Parser YAML leve em puro Python como fallback caso PyYAML não esteja instalado
    no ambiente local ou container.
    """
    lines_clean: list[tuple[int, str]] = []
    for raw_line in text.splitlines():
        without_comment = _strip_inline_comment(raw_line).rstrip()
        if not without_comment.strip():
            continue
        indent = len(without_comment) - len(without_comment.lstrip(" "))
        lines_clean.append((indent, without_comment.strip()))

    root: dict[str, Any] = {}
    stack: list[tuple[int, Any]] = [(-1, root)]

    for idx, (indent, content) in enumerate(lines_clean):
        while len(stack) > 1 and indent <= stack[-1][0]:
            stack.pop()
        parent = stack[-1][1]

        if content.startswith("- "):
            item_val = _parse_scalar(content[2:])
            if isinstance(parent, list):
                parent.append(item_val)
            continue

        if ":" in content:
            key, _, rest = content.partition(":")
            key = key.strip()
            rest = rest.strip()
            if not rest:
                # Verifica se o próximo elemento é lista ou dicionário
                next_is_list = False
                if idx + 1 < len(lines_clean):
                    next_indent, next_content = lines_clean[idx + 1]
                    if next_indent > indent and next_content.startswith("- "):
                        next_is_list = True
                container: Any = [] if next_is_list else {}
                if isinstance(parent, dict):
                    parent[key] = container
                stack.append((indent, container))
            else:
                if isinstance(parent, dict):
                    parent[key] = _parse_scalar(rest)

    return root


def carregar_configuracao(caminho_yaml: str | Path) -> dict[str, Any]:
    """Carrega o arquivo YAML de configuração do Data Quality."""
    path = Path(caminho_yaml)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo de configuração não encontrado: {path}")

    conteudo = path.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore

        dados = yaml.safe_load(conteudo)
        return dados if isinstance(dados, dict) else {}
    except ImportError:
        return _fallback_yaml_load(conteudo)
