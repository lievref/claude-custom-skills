#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import argparse
import csv
import datetime as dt
import os
import time
from typing import Dict, Tuple, Optional, List

import requests

API_BASE = "https://servicos.dnit.gov.br/sgplan/apigeo/snv/listarversaopordt"


# =========================
# Exceções
# =========================
class SNVDataOutOfRangeError(ValueError):
    """A data solicitada não está coberta pelo CSV local."""
    pass


# =========================
# Núcleo: chamadas à API
# =========================
def api_get_versao(data: dt.date, cache: Dict[dt.date, str],
                   timeout: int = 10, max_retries: int = 3, pause: float = 0.5) -> str:
    """Consulta a versão do SNV vigente em uma data (com cache e retries)."""
    if data in cache:
        return cache[data]

    params = {"data": data.isoformat()}
    last_err: Optional[Exception] = None
    for attempt in range(1, max_retries + 1):
        try:
            r = requests.get(API_BASE, params=params, timeout=timeout)
            r.raise_for_status()
            j = r.json()
            versao = j.get("versao")
            if not versao:
                raise ValueError(f"Resposta sem campo 'versao' para {data}")
            cache[data] = versao
            return versao
        except Exception as e:
            last_err = e
            time.sleep(pause * attempt)
    raise RuntimeError(f"Falha ao consultar API para {data}: {last_err}")


# =========================
# Utilidades de data
# =========================
def first_day_of_month(d: dt.date) -> dt.date:
    return d.replace(day=1)


def add_months(d: dt.date, n: int) -> dt.date:
    year = d.year + (d.month - 1 + n) // 12
    month = (d.month - 1 + n) % 12 + 1
    day = min(
        d.day,
        [31, 29 if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0) else 28,
         31, 30, 31, 30, 31, 31, 30, 31, 30, 31][month - 1],
    )
    return dt.date(year, month, day)


def month_iter(start: dt.date, end: dt.date):
    cur = start
    while cur <= end:
        yield cur
        cur = add_months(cur, 1)


def mid_date(a: dt.date, b: dt.date) -> dt.date:
    mid_ord = (a.toordinal() + b.toordinal()) // 2
    return dt.date.fromordinal(mid_ord)


# =========================
# Divide e conquista: todas as transições de versão num intervalo
# =========================
def encontrar_transicoes(
    lo: dt.date, hi: dt.date,
    versao_lo: str, versao_hi: str,
    cache: Dict[dt.date, str],
) -> List[Tuple[dt.date, dt.date, str]]:
    """
    Retorna todos os segmentos [(d_ini, d_fim, versao)] no intervalo fechado [lo, hi].
    Pré-condição: versao(lo) == versao_lo, versao(hi) == versao_hi (já no cache).
    Correto mesmo quando há múltiplas transições de versão no intervalo.
    """
    if versao_lo == versao_hi:
        return [(lo, hi, versao_lo)]

    if (hi - lo).days <= 1:
        # Dias adjacentes com versões diferentes — corte exato aqui
        segs: List[Tuple[dt.date, dt.date, str]] = [(lo, lo, versao_lo)]
        if hi > lo:
            segs.append((hi, hi, versao_hi))
        return segs

    mid = mid_date(lo, hi)
    mid_next = mid + dt.timedelta(days=1)
    versao_mid      = api_get_versao(mid,      cache)
    versao_mid_next = api_get_versao(mid_next, cache)

    left  = encontrar_transicoes(lo,       mid,  versao_lo,       versao_mid,      cache)
    right = encontrar_transicoes(mid_next, hi,   versao_mid_next, versao_hi,       cache)

    # Fundir bordas contíguas com a mesma versão
    if left and right and left[-1][2] == right[0][2]:
        merged = (left[-1][0], right[0][1], left[-1][2])
        return left[:-1] + [merged] + right[1:]

    return left + right


# =========================
# CSV I/O
# =========================
def ler_csv(csv_path: str) -> List[Tuple[dt.date, dt.date, str]]:
    """Lê o CSV inteiro (se existir) e retorna lista [(data_inicial, data_final, versao)]."""
    if not os.path.exists(csv_path):
        return []
    linhas = []
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = list(csv.reader(f))
        for r in rows[1:]:
            if len(r) < 3:
                continue
            try:
                linhas.append((dt.date.fromisoformat(r[0]), dt.date.fromisoformat(r[1]), r[2]))
            except Exception:
                continue
    linhas.sort(key=lambda t: t[0])
    return linhas


def escrever_csv(csv_path: str, linhas: List[Tuple[str, str, str]]):
    """Escreve CSV sem linha em branco final."""
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        f.write("data_inicial,data_final,versao\n")
        for i, linha in enumerate(linhas):
            f.write(",".join(linha))
            if i < len(linhas) - 1:
                f.write("\n")


# =========================
# Construtor/Atualizador da Tabela
# =========================
def construir_tabela(inicio_inicial: dt.date, dia_atual: dt.date, csv_path: str,
                     timeout: int = 10, max_retries: int = 3, forcar: bool = False, quiet: bool = False):
    """Cria/atualiza tabela CSV incrementalmente."""
    cache: Dict[dt.date, str] = {}
    linhas_existentes = [] if forcar else ler_csv(csv_path)
    linhas_saida: List[Tuple[str, str, str]] = []

    if not linhas_existentes:
        versao_corrente = api_get_versao(inicio_inicial, cache, timeout, max_retries)
        data_inicial_corrente = inicio_inicial
        start_month = dt.date(2015, 2, 1)
    else:
        for d0, d1, ver in linhas_existentes[:-1]:
            linhas_saida.append((d0.isoformat(), d1.isoformat(), ver))
        data_inicial_corrente, data_final_corrente, versao_corrente = linhas_existentes[-1]
        start_month = first_day_of_month(max(data_final_corrente + dt.timedelta(days=1), dt.date(2015, 2, 1)))

    fim_iter = first_day_of_month(dia_atual)
    if not linhas_existentes:
        data_inicial_corrente = inicio_inicial

    for d in month_iter(start_month, fim_iter):
        v = api_get_versao(d, cache, timeout, max_retries)
        if v != versao_corrente:
            lo = max(first_day_of_month(d) - dt.timedelta(days=31), data_inicial_corrente)
            if api_get_versao(lo, cache, timeout, max_retries) != versao_corrente:
                lo = data_inicial_corrente
            hi = d - dt.timedelta(days=1)
            # Divide e conquista: detecta todas as transições no intervalo,
            # inclusive múltiplas versões dentro do mesmo mês.
            segmentos = encontrar_transicoes(lo, hi, versao_corrente, v, cache)
            # Fecha todos os segmentos completos, exceto o último (ainda pode crescer)
            for seg_ini, seg_fim, seg_ver in segmentos[:-1]:
                linhas_saida.append((seg_ini.isoformat(), seg_fim.isoformat(), seg_ver))
            versao_corrente      = segmentos[-1][2]
            data_inicial_corrente = segmentos[-1][0]

    linhas_saida.append((data_inicial_corrente.isoformat(), dia_atual.isoformat(), versao_corrente))
    escrever_csv(csv_path, linhas_saida)
    if not quiet:
        print(f"Tabela atualizada até {dia_atual.isoformat()} ({len(linhas_saida)} versões registradas)")
        print(f"Arquivo salvo em: {csv_path}")


# =========================
# Helpers de parsing de data
# =========================
def _to_date(data) -> dt.date:
    """Aceita datetime.date, datetime.datetime ou string e retorna datetime.date."""
    if isinstance(data, dt.date) and not isinstance(data, dt.datetime):
        return data
    if isinstance(data, dt.datetime):
        return data.date()
    if isinstance(data, str):
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y"):
            try:
                return dt.datetime.strptime(data, fmt).date()
            except ValueError:
                continue
        raise ValueError(f"Formato de data inválido: {data!r}")
    raise TypeError(f"Tipo de data não suportado: {type(data)}")


# =========================
# Consulta de versão por data (CSV/API)
# =========================
def versao_por_data_csv(data, csv_path: str) -> Optional[str]:
    """
    Retorna a versão do SNV vigente em 'data' consultando o CSV (offline).
    Levanta SNVDataOutOfRangeError se a data estiver fora do intervalo do CSV.
    """
    data = _to_date(data)
    linhas = ler_csv(csv_path)
    if not linhas:
        return None

    inicio_csv, fim_csv = linhas[0][0], linhas[-1][1]
    if data < inicio_csv or data > fim_csv:
        raise SNVDataOutOfRangeError(
            f"Data {data.isoformat()} fora do intervalo do CSV "
            f"({inicio_csv.isoformat()} a {fim_csv.isoformat()})."
        )

    # busca binária
    lo, hi = 0, len(linhas) - 1
    while lo <= hi:
        mid = (lo + hi) // 2
        d0, d1, ver = linhas[mid]
        if d0 <= data <= d1:
            return ver
        if data < d0:
            hi = mid - 1
        else:
            lo = mid + 1
    return None


def versao_por_data_api(data, timeout: int = 10, max_retries: int = 3) -> str:
    """Retorna a versão do SNV via API (online). Aceita data como string."""
    data = _to_date(data)
    return api_get_versao(data, cache={}, timeout=timeout, max_retries=max_retries)


def versao_por_data(data, csv_path: Optional[str] = None,
                    timeout: int = 10, max_retries: int = 3,
                    strict_csv: bool = False) -> str:
    """
    Conveniência: tenta CSV se informado/existir; caso contrário, usa a API.
    - Aceita data como string ('YYYY-MM-DD', 'YYYY/MM/DD', 'DD/MM/YYYY'), datetime.date ou datetime.datetime.
    - Se strict_csv=True e a data estiver fora do CSV, relança erro; caso contrário, faz fallback para a API.
    """
    data = _to_date(data)

    if csv_path and os.path.exists(csv_path):
        try:
            ver = versao_por_data_csv(data, csv_path)
            if ver is not None:
                return ver
        except SNVDataOutOfRangeError:
            if strict_csv:
                raise
            return versao_por_data_api(data, timeout=timeout, max_retries=max_retries)

    return versao_por_data_api(data, timeout=timeout, max_retries=max_retries)


# =========================
# CLI
# =========================
def main():
    parser = argparse.ArgumentParser(description="Gera/atualiza CSV com versões do SNV (DNIT).")
    parser.add_argument("-o", "--output", nargs="?", const="snv_versoes.csv",
                        help="Caminho completo do CSV (opcional). Padrão: snv_versoes.csv no diretório atual.")
    parser.add_argument("--inicio", default="2015-01-01", help="Data inicial absoluta")
    parser.add_argument("--hoje", default=None, help="Data de corte (padrão: hoje)")
    parser.add_argument("--timeout", type=int, default=10, help="Timeout por requisição")
    parser.add_argument("--retries", type=int, default=3, help="Tentativas por requisição")
    parser.add_argument("--forcar", action="store_true", help="Reconstrói tabela do zero")
    parser.add_argument("--quiet", action="store_true", help="Suprime mensagens de console")
    args = parser.parse_args()

    csv_path = args.output if args.output else os.path.join(os.getcwd(), "snv_versoes.csv")
    inicio_inicial = dt.date.fromisoformat(args.inicio)
    dia_atual = dt.date.fromisoformat(args.hoje) if args.hoje else dt.date.today()

    construir_tabela(inicio_inicial, dia_atual, csv_path,
                     timeout=args.timeout, max_retries=args.retries,
                     forcar=args.forcar, quiet=args.quiet)


if __name__ == "__main__":
    main()
