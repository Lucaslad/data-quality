"""Carregador e validador de schema de configuração YAML para o motor de Data Quality."""

from __future__ import annotations

import ast
import logging
from pathlib import Path
from typing import Any

logger = logging.getLogger("dq_engine")

CHAVES_RAIZ_VALIDAS = {"configuracoes_gerais", "tabelas"}

CHAVES_CFG_GERAIS_VALIDAS = {
    "diretorio_origem",
    "diretorio_saida",
    "data_referencia_padrao",
    "leitura_incremental",
    "modo_incremental_producao",
    "janela_leitura_recente_diaria",
    "janela_leitura_recente_mensal",
    "modo_streaming_memoria",
    "particionar_metadados",
    "formato_log",
    "nivel_log",
    "roteamento_e_score",
    "integracao_catalogo",
}

CHAVES_TABELA_VALIDAS = {
    "ativo",
    "descricao",
    "caminho_relativo",
    "coluna_particao",
    "tipo_particao",
    "chaves_primarias",
    "checks",
}

CHAVES_CHECKS_VALIDAS = {"particao", "maturacao_datas", "colunas"}

CHAVES_CHECKS_PARTICAO_VALIDAS = {
    "volumetria_historica",
    "evolucao_fotografias",
    "consistencia_data_particao",
    "consistencia_schema",
    "freshness_lacunas",
}

CHAVES_CHECKS_COLUNAS_VALIDAS = {
    "completude",
    "unicidade",
    "dominio",
    "intervalo_numerico",
    "formato_regex",
    "consistencia_entre_colunas",
    "digito_verificador",
    "consistencia_schema",
}

TIPOS_PARTICAO_VALIDOS = {"mensal", "diaria"}


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


def validar_configuracao(
    dados: dict[str, Any], estrito: bool = False
) -> list[str]:
    """
    Valida o schema do dicionário carregado do `dq_config.yaml`.
    Detecta chaves desconhecidas (erros de digitação) e tipos inválidos.
    Retorna lista de avisos encontrados; se `estrito=True`, levanta `ValueError` no primeiro aviso.
    """
    if not isinstance(dados, dict):
        raise ValueError("A configuração YAML deve ser um dicionário na raiz.")

    avisos: list[str] = []

    for k_raiz in dados.keys():
        if k_raiz not in CHAVES_RAIZ_VALIDAS:
            avisos.append(
                f"Chave raiz desconhecida no YAML: `{k_raiz}` (esperado: {sorted(CHAVES_RAIZ_VALIDAS)})"
            )

    cfg_gerais = dados.get("configuracoes_gerais", {})
    if cfg_gerais is not None and not isinstance(cfg_gerais, dict):
        raise ValueError("`configuracoes_gerais` deve ser um dicionário.")
    if isinstance(cfg_gerais, dict):
        for k_g in cfg_gerais.keys():
            if k_g not in CHAVES_CFG_GERAIS_VALIDAS:
                avisos.append(
                    f"Chave desconhecida em `configuracoes_gerais`: `{k_g}`"
                )

    tabelas = dados.get("tabelas", {})
    if tabelas is not None and not isinstance(tabelas, dict):
        raise ValueError("`tabelas` deve ser um dicionário mapeando o nome de cada tabela.")

    if isinstance(tabelas, dict):
        for nome_tab, cfg_tab in tabelas.items():
            if not isinstance(cfg_tab, dict):
                avisos.append(f"Configuração da tabela `{nome_tab}` não é um dicionário.")
                continue

            for k_t in cfg_tab.keys():
                if k_t not in CHAVES_TABELA_VALIDAS:
                    avisos.append(
                        f"Chave desconhecida na tabela `{nome_tab}`: `{k_t}`"
                    )

            tipo_part = str(cfg_tab.get("tipo_particao", "mensal")).lower()
            if tipo_part not in TIPOS_PARTICAO_VALIDOS:
                raise ValueError(
                    f"Valor inválido de `tipo_particao` em `{nome_tab}`: `{tipo_part}` "
                    f"(valores aceitos: {sorted(TIPOS_PARTICAO_VALIDOS)})"
                )

            checks_tab = cfg_tab.get("checks", {})
            if isinstance(checks_tab, dict):
                for k_chk in checks_tab.keys():
                    if k_chk not in CHAVES_CHECKS_VALIDAS:
                        avisos.append(
                            f"Nível de check desconhecido em `{nome_tab}.checks`: `{k_chk}`"
                        )
                chk_part = checks_tab.get("particao", {})
                if isinstance(chk_part, dict):
                    for k_cp in chk_part.keys():
                        if k_cp not in CHAVES_CHECKS_PARTICAO_VALIDAS:
                            avisos.append(
                                f"Check de partição desconhecido em `{nome_tab}.checks.particao`: `{k_cp}`"
                            )
                chk_cols = checks_tab.get("colunas", {})
                if isinstance(chk_cols, dict):
                    for k_cc in chk_cols.keys():
                        if k_cc not in CHAVES_CHECKS_COLUNAS_VALIDAS:
                            avisos.append(
                                f"Check de coluna desconhecido em `{nome_tab}.checks.colunas`: `{k_cc}`"
                            )

    for av in avisos:
        logger.warning("[!] Aviso de validação do YAML: %s", av)

    if estrito and avisos:
        raise ValueError("Erros de validação no YAML: " + "; ".join(avisos))

    return avisos


def carregar_configuracao(
    caminho_yaml: str | Path, validar: bool = True, estrito: bool = False
) -> dict[str, Any]:
    """Carrega e valida o arquivo YAML de configuração do Data Quality."""
    path = Path(caminho_yaml)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo de configuração não encontrado: {path}")

    conteudo = path.read_text(encoding="utf-8")
    try:
        import yaml  # type: ignore

        dados_raw = yaml.safe_load(conteudo)
        dados = dados_raw if isinstance(dados_raw, dict) else {}
    except ImportError:
        dados = _fallback_yaml_load(conteudo)

    if validar:
        validar_configuracao(dados, estrito=estrito)

    return dados
