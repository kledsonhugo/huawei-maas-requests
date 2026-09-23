#!/usr/bin/env python3
"""
Teste de carga para modelos LLM no Huawei MaaS (API OpenAI-compatible).

Executa chamadas em paralelo via streaming e coleta métricas de mercado
usadas para avaliar inferência de LLM:

    TTFT    (Time To First Token)   -> tempo até o primeiro token
    TPOT    (Time Per Output Token) -> tempo médio entre tokens
    E2E     (End-to-End Latency)    -> tempo total da requisição
    Output tokens/sec               -> velocidade de geração
    QPS     (Queries Per Second)    -> requisições por segundo (atingido)
    TPM     (Tokens Per Minute)     -> tokens processados por minuto (atingido)
    P50/P90/P99                     -> distribuição da latência (cauda)
    Error rate                      -> % de requisições que falharam
    Tokens/request                  -> tokens de entrada + saída por requisição

Requisitos:
    - Python 3.8+
    - pip install requests
    - Variável de ambiente MAAS_API_KEY definida:
        export MAAS_API_KEY="sua-chave-aqui"

Uso:
    python maas_parallel_test.py
"""

import os
import json
import math
import time
import threading
import statistics
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor, as_completed

import requests

# ---------------------------------------------------------------------------
# Configuração
# ---------------------------------------------------------------------------
URL = "https://api-ap-southeast-1.modelarts-maas.com/openai/v1/chat/completions"
API_KEY = os.environ.get("MAAS_API_KEY") or os.environ.get("API_KEY")

MODELS = ["glm-5.2", "glm-5.3"]
CALLS_PER_MODEL = 100
MAX_WORKERS = len(MODELS) * CALLS_PER_MODEL  # 200 chamadas em paralelo
TIMEOUT = 120  # segundos por chamada
MAX_RETRIES = 5  # tentativas por chamada em caso de HTTP 429 (rate limit)
RATE_LIMIT_RPS = 4  # limite do endpoint: 4 requisições por segundo
REPORT_PATH = os.path.join(os.path.dirname(os.path.abspath(__file__)), "maas_report.html")

# Prompt do teste. Precisa gerar tokens suficientes para que TPOT e
# output tokens/sec sejam significativos (com "Responda apenas: OK"
# haveria ~1 token e nenhuma métrica de geração seria mensurável).
PROMPT = ("Escreva um parágrafo curto, de 3 a 4 frases, "
          "explicando o que é inteligência artificial.")

# Alguns endpoints OpenAI-compatíveis não suportam "stream_options".
# Se a primeira chamada retornar HTTP 400, desabilitamos globalmente e
# passamos a aproximar a contagem de tokens pelos chunks recebidos.
INCLUDE_USAGE = {"enabled": True}

# Paleta de cores por modelo (usada no relatório HTML)
MODEL_COLORS = {
    "glm-5.2": "#6366f1",
    "glm-5.3": "#10b981",
}
DEFAULT_COLOR = "#64748b"


class RateLimiter:
    """Token bucket compartilhado entre todas as threads.

    Garante que as requisições sejam enviadas no máximo a RATE_LIMIT_RPS
    por segundo, evitando HTTP 429. Cada requisição "compra" um token;
    tokens são repostos a cada 1/RATE_LIMIT_RPS segundos.
    """

    def __init__(self, rps: float):
        self._interval = 1.0 / rps
        self._lock = threading.Lock()
        self._next_slot = 0.0  # timestamp do próximo slot disponível

    def acquire(self) -> None:
        """Bloqueia até que um slot de envio esteja disponível."""
        with self._lock:
            now = time.monotonic()
            # Se o próximo slot já passou, usa o tempo atual (evita drift)
            self._next_slot = max(self._next_slot, now)
            wait = self._next_slot - now
            self._next_slot += self._interval
        if wait > 0:
            time.sleep(wait)


# Rate limiter global: compartilhado por todas as chamadas dos dois modelos
rate_limiter = RateLimiter(RATE_LIMIT_RPS)

HEADERS = {
    "Authorization": f"Bearer {API_KEY}",
    "Content-Type": "application/json",
}


def call_maas(model: str, call_id: int) -> dict:
    """Faz uma chamada em streaming ao MaaS coletando TTFT, TPOT e usage.

    Fluxo:
        1. Envia a requisição com stream=True
        2. Marca o instante do primeiro token (TTFT) — inclui tokens de
           raciocínio (reasoning_content), padrão de mercado para
           modelos de raciocínio (o primeiro token já é resposta do modelo)
        3. Conta os tokens subsequentes (para TPOT e tokens/sec)
        4. Lê o bloco "usage" do último chunk (se o endpoint suportar)
    """
    payload = {
        "model": model,
        "messages": [
            {"role": "user", "content": PROMPT}
        ],
        "stream": True,
    }
    if INCLUDE_USAGE["enabled"]:
        payload["stream_options"] = {"include_usage": True}

    start = None
    retries = 0
    while True:
        ttft = None
        chunk_times = []
        content_parts = []
        usage = None
        try:
            # Respeita o rate limit antes de cada envio (inclui retries).
            # O cronômetro inicia APÓS o acquire para que TTFT/E2E meçam
            # apenas o tempo do servidor, sem a fila do rate limiter.
            rate_limiter.acquire()
            start = time.perf_counter()
            response = requests.post(
                URL, headers=HEADERS, json=payload,
                timeout=TIMEOUT, stream=True,
            )

            if response.status_code != 200:
                elapsed = time.perf_counter() - start
                # stream_options pode não ser suportado: desabilita e refaz
                if (response.status_code == 400 and INCLUDE_USAGE["enabled"]
                        and "stream_options" in response.text):
                    INCLUDE_USAGE["enabled"] = False
                    payload.pop("stream_options", None)
                    continue
                if response.status_code == 429 and retries < MAX_RETRIES:
                    retries += 1
                    time.sleep(min(2 ** retries, 30))
                    continue
                return {
                    "model": model, "call_id": call_id, "ok": False,
                    "status": response.status_code, "latency": elapsed,
                    "ttft": None, "tpot": None, "tokens_per_sec": None,
                    "input_tokens": None, "output_tokens": None,
                    "reasoning_tokens": None,
                    "content": None, "error": response.text[:200],
                    "retries": retries,
                }

            # Consome o stream SSE linha a linha
            for line in response.iter_lines(decode_unicode=True):
                if not line or not line.startswith("data:"):
                    continue
                data_str = line[len("data:"):].strip()
                if data_str == "[DONE]":
                    break
                try:
                    chunk = json.loads(data_str)
                except json.JSONDecodeError:
                    continue

                if chunk.get("usage"):
                    usage = chunk["usage"]

                choices = chunk.get("choices") or []
                if not choices:
                    continue
                delta = choices[0].get("delta") or {}
                # Modelos de raciocínio (GLM) emitem reasoning_content
                # antes do content. O TTFT de mercado conta o primeiro
                # token QUALQUER (raciocínio ou conteúdo).
                piece = delta.get("content") or delta.get("reasoning_content")
                if piece:
                    now = time.perf_counter()
                    if ttft is None:
                        ttft = now - start
                    else:
                        chunk_times.append(now - start)
                    if delta.get("content"):
                        content_parts.append(delta["content"])

            elapsed = time.perf_counter() - start
            content = "".join(content_parts).strip()

            # -----------------------------------------------------------------
            # Deriva as métricas de geração
            # -----------------------------------------------------------------
            # Preferir sempre a contagem oficial do "usage" (o servidor pode
            # agrupar vários tokens por chunk SSE, o que tornaria a contagem
            # de chunks uma subestimativa de TPOT/tokens-per-sec).
            output_tokens = usage.get("completion_tokens") if usage else None
            if output_tokens is None and chunk_times:
                # Fallback: aproxima por chunks recebidos (1 chunk ~ 1 token)
                output_tokens = len(chunk_times) + 1

            tpot = None
            if output_tokens and output_tokens > 1 and ttft is not None:
                gen_time = elapsed - ttft
                tpot = gen_time / (output_tokens - 1)

            tokens_per_sec = None
            if output_tokens and ttft is not None and elapsed > ttft:
                tokens_per_sec = output_tokens / (elapsed - ttft)

            # Tokens de raciocínio (se reportados pelo servidor)
            reasoning_tokens = None
            if usage:
                details = usage.get("completion_tokens_details") or {}
                reasoning_tokens = details.get("reasoning_tokens")

            return {
                "model": model, "call_id": call_id, "ok": True,
                "status": 200, "latency": elapsed,
                "ttft": ttft, "tpot": tpot, "tokens_per_sec": tokens_per_sec,
                "input_tokens": usage.get("prompt_tokens") if usage else None,
                "output_tokens": output_tokens,
                "reasoning_tokens": reasoning_tokens,
                "content": content, "error": None,
                "retries": retries,
            }

        except requests.RequestException as exc:
            elapsed = time.perf_counter() - start if start else 0.0
            return {
                "model": model, "call_id": call_id, "ok": False,
                "status": None, "latency": elapsed,
                "ttft": None, "tpot": None, "tokens_per_sec": None,
                "input_tokens": None, "output_tokens": None,
                "reasoning_tokens": None,
                "content": None, "error": str(exc),
                "retries": retries,
            }


def percentile(values: list, pct: float) -> float:
    """Percentil por interpolação linear (mesmo método do numpy)."""
    if not values:
        return 0.0
    sorted_vals = sorted(values)
    if len(sorted_vals) == 1:
        return sorted_vals[0]
    rank = (pct / 100) * (len(sorted_vals) - 1)
    lower = math.floor(rank)
    upper = math.ceil(rank)
    if lower == upper:
        return sorted_vals[int(rank)]
    frac = rank - lower
    return sorted_vals[lower] * (1 - frac) + sorted_vals[upper] * frac


def model_stats(results: list, model: str, total_elapsed: float) -> dict:
    """Calcula as métricas de mercado agregadas de um modelo."""
    model_results = [r for r in results if r["model"] == model]
    ok_results = [r for r in model_results if r["ok"]]
    latencies = [r["latency"] for r in ok_results]
    ttfts = [r["ttft"] for r in ok_results if r["ttft"] is not None]
    tpots = [r["tpot"] for r in ok_results if r["tpot"] is not None]
    tps_list = [r["tokens_per_sec"] for r in ok_results if r["tokens_per_sec"]]
    in_tokens = [r["input_tokens"] for r in ok_results if r["input_tokens"]]
    out_tokens = [r["output_tokens"] for r in ok_results if r["output_tokens"]]
    reasoning = [r["reasoning_tokens"] for r in ok_results if r.get("reasoning_tokens")]

    total_tokens = sum(in_tokens) + sum(out_tokens)

    return {
        "model": model,
        "total": len(model_results),
        "ok": len(ok_results),
        "error_rate": (len(model_results) - len(ok_results)) / len(model_results) * 100
                      if model_results else 0,
        "retries": sum(r["retries"] for r in model_results),
        # Latência E2E
        "mean": statistics.mean(latencies) if latencies else 0,
        "min": min(latencies) if latencies else 0,
        "max": max(latencies) if latencies else 0,
        "stdev": statistics.stdev(latencies) if len(latencies) > 1 else 0,
        "p50": percentile(latencies, 50),
        "p90": percentile(latencies, 90),
        "p99": percentile(latencies, 99),
        # TTFT / TPOT / throughput
        "ttft_mean": statistics.mean(ttfts) if ttfts else 0,
        "ttft_p50": percentile(ttfts, 50),
        "ttft_p90": percentile(ttfts, 90),
        "tpot_mean": statistics.mean(tpots) if tpots else 0,
        "tokens_per_sec_mean": statistics.mean(tps_list) if tps_list else 0,
        # Capacidade atingida no teste (não é o limite do endpoint)
        "qps": len(ok_results) / total_elapsed if total_elapsed > 0 else 0,
        "tpm": (total_tokens / total_elapsed) * 60 if total_elapsed > 0 else 0,
        # Tokens por requisição
        "avg_input_tokens": statistics.mean(in_tokens) if in_tokens else 0,
        "avg_output_tokens": statistics.mean(out_tokens) if out_tokens else 0,
        "avg_reasoning_tokens": statistics.mean(reasoning) if reasoning else 0,
        "total_tokens": total_tokens,
        "color": MODEL_COLORS.get(model, DEFAULT_COLOR),
    }


def build_html(results: list, total_elapsed: float) -> str:
    """Gera o relatório HTML completo com métricas de mercado."""
    stats = [model_stats(results, m, total_elapsed) for m in MODELS]
    now = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    total_calls = len(results)
    total_ok = sum(1 for r in results if r["ok"])

    def fmt_ms(seconds: float) -> str:
        """Formata segundos como ms quando o valor for pequeno."""
        if seconds is None:
            return "—"
        if seconds < 1:
            return f"{seconds * 1000:.0f}ms"
        return f"{seconds:.2f}s"

    # --- Cards de resumo geral -------------------------------------------------
    cards = f"""
    <div class="card">
        <div class="card-value">{total_ok}/{total_calls}</div>
        <div class="card-label">Chamadas bem-sucedidas</div>
    </div>
    <div class="card">
        <div class="card-value">{(total_calls - total_ok) / total_calls * 100:.1f}%</div>
        <div class="card-label">Error rate (geral)</div>
    </div>
    <div class="card">
        <div class="card-value">{total_elapsed:.2f}s</div>
        <div class="card-label">Tempo total (paralelo)</div>
    </div>"""
    for s in stats:
        cards += f"""
    <div class="card" style="border-top: 4px solid {s['color']}">
        <div class="card-value" style="color:{s['color']}">{fmt_ms(s['ttft_mean'])}</div>
        <div class="card-label">{s['model']} · TTFT médio</div>
    </div>"""

    # --- Tabela de métricas de mercado ------------------------------------------
    metrics_rows = ""
    for s in stats:
        metrics_rows += f"""
        <tr>
            <td><span class="dot" style="background:{s['color']}"></span>{s['model']}</td>
            <td>{fmt_ms(s['ttft_mean'])}</td>
            <td>{fmt_ms(s['ttft_p50'])} / {fmt_ms(s['ttft_p90'])}</td>
            <td>{fmt_ms(s['tpot_mean'])}</td>
            <td>{s['tokens_per_sec_mean']:.1f}</td>
            <td>{fmt_ms(s['mean'])}</td>
            <td>{fmt_ms(s['p50'])} / {fmt_ms(s['p90'])} / {fmt_ms(s['p99'])}</td>
        </tr>"""

    # --- Tabela de capacidade e confiabilidade ----------------------------------
    capacity_rows = ""
    for s in stats:
        capacity_rows += f"""
        <tr>
            <td><span class="dot" style="background:{s['color']}"></span>{s['model']}</td>
            <td>{s['qps']:.2f}</td>
            <td>{s['tpm']:.0f}</td>
            <td>{s['ok']}/{s['total']}</td>
            <td>{s['error_rate']:.1f}%</td>
            <td>{s['avg_input_tokens']:.0f}</td>
            <td>{s['avg_output_tokens']:.0f}</td>
            <td>{s['avg_reasoning_tokens']:.0f}</td>
            <td>{s['total_tokens']}</td>
        </tr>"""

    # --- Gráfico de barras: E2E e TTFT por chamada -------------------------------
    all_latencies = [r["latency"] for r in results if r["ok"]]
    max_latency = max(all_latencies) if all_latencies else 1
    chart_bars = ""
    for model in MODELS:
        model_results = sorted(
            [r for r in results if r["model"] == model and r["ok"]],
            key=lambda r: r["call_id"],
        )
        color = MODEL_COLORS.get(model, DEFAULT_COLOR)
        bars = ""
        for r in model_results:
            height_pct = (r["latency"] / max_latency) * 100
            ttft_pct = (r["ttft"] / max_latency) * 100 if r["ttft"] else 0
            title_text = (f"Chamada {r['call_id']}: E2E {r['latency']:.2f}s, "
                          f"TTFT {fmt_ms(r['ttft'])}, "
                          f"{r['output_tokens'] or 0} tokens de saída")
            bars += f"""
            <div class="bar-group">
                <div class="bar-value">{r['latency']:.1f}s</div>
                <div class="bar" style="height:{height_pct:.1f}%;background:{color}" title="{title_text}">
                    <div class="bar-ttft" style="height:{ttft_pct / height_pct * 100 if height_pct else 0:.1f}%"></div>
                </div>
                <div class="bar-label">#{r['call_id']:02d}</div>
            </div>"""
        chart_bars += f"""
        <div class="chart-section">
            <h3><span class="dot" style="background:{color}"></span>{model}
                <span class="chart-legend">parte escura = TTFT · barra inteira = E2E</span></h3>
            <div class="chart">{bars}</div>
        </div>"""

    # --- Tabela detalhada -------------------------------------------------------
    detail_rows = ""
    for r in sorted(results, key=lambda r: (r["model"], r["call_id"])):
        status_badge = ('<span class="badge ok">OK</span>' if r["ok"]
                        else f'<span class="badge err">ERRO</span>')
        color = MODEL_COLORS.get(r["model"], DEFAULT_COLOR)
        detail_rows += f"""
        <tr>
            <td><span class="dot" style="background:{color}"></span>{r['model']}</td>
            <td>#{r['call_id']:02d}</td>
            <td>{status_badge}</td>
            <td>{r['status'] or '—'}</td>
            <td>{fmt_ms(r['ttft'])}</td>
            <td>{fmt_ms(r['tpot'])}</td>
            <td>{r['tokens_per_sec']:.1f}</td>
            <td>{fmt_ms(r['latency'])}</td>
            <td>{r['input_tokens'] or '—'}</td>
            <td>{r['output_tokens'] or '—'}</td>
            <td>{r.get('reasoning_tokens') or '—'}</td>
            <td>{r['retries']}</td>
        </tr>"""

    return f"""<!DOCTYPE html>
<html lang="pt-br">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Relatório MaaS — Teste Paralelo</title>
<style>
    * {{ margin: 0; padding: 0; box-sizing: border-box; }}
    body {{
        font-family: 'Segoe UI', system-ui, -apple-system, sans-serif;
        background: #f1f5f9;
        color: #1e293b;
        padding: 2rem;
        line-height: 1.5;
    }}
    .container {{ max-width: 1100px; margin: 0 auto; }}
    h1 {{
        font-size: 1.6rem;
        margin-bottom: 0.25rem;
        background: linear-gradient(90deg, #6366f1, #10b981);
        -webkit-background-clip: text;
        background-clip: text;
        -webkit-text-fill-color: transparent;
    }}
    .subtitle {{ color: #64748b; font-size: 0.9rem; margin-bottom: 1.5rem; }}
    .cards {{
        display: grid;
        grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
        gap: 1rem;
        margin-bottom: 1.5rem;
    }}
    .card {{
        background: white;
        border-radius: 12px;
        padding: 1.25rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08);
        border-top: 4px solid #6366f1;
    }}
    .card-value {{ font-size: 1.9rem; font-weight: 700; }}
    .card-label {{ color: #64748b; font-size: 0.85rem; margin-top: 0.25rem; }}
    .section {{
        background: white;
        border-radius: 12px;
        padding: 1.5rem;
        box-shadow: 0 1px 3px rgba(0,0,0,0.08);
        margin-bottom: 1.5rem;
    }}
    .section h2 {{
        font-size: 1.1rem;
        margin-bottom: 1rem;
        color: #334155;
    }}
    .section h3 {{
        font-size: 1rem;
        margin-bottom: 0.75rem;
        color: #334155;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }}
    table {{
        width: 100%;
        border-collapse: collapse;
        font-size: 0.9rem;
    }}
    th, td {{
        padding: 0.6rem 0.75rem;
        text-align: left;
        border-bottom: 1px solid #e2e8f0;
    }}
    th {{
        background: #f8fafc;
        font-weight: 600;
        color: #475569;
        font-size: 0.8rem;
        text-transform: uppercase;
        letter-spacing: 0.03em;
    }}
    tr:hover td {{ background: #f8fafc; }}
    .dot {{
        display: inline-block;
        width: 10px; height: 10px;
        border-radius: 50%;
        margin-right: 0.5rem;
    }}
    .badge {{
        padding: 0.15rem 0.6rem;
        border-radius: 999px;
        font-size: 0.75rem;
        font-weight: 600;
    }}
    .badge.ok {{ background: #d1fae5; color: #065f46; }}
    .badge.err {{ background: #fee2e2; color: #991b1b; }}
    .mono {{
        font-family: 'Cascadia Code', 'Fira Code', monospace;
        font-size: 0.8rem;
        max-width: 320px;
        overflow: hidden;
        text-overflow: ellipsis;
        white-space: nowrap;
    }}
    .chart-section {{ margin-bottom: 2rem; }}
    .chart-section:last-child {{ margin-bottom: 0; }}
    .chart {{
        display: flex;
        align-items: flex-end;
        gap: 0.5rem;
        height: 220px;
        padding-top: 1.5rem;
        border-bottom: 2px solid #e2e8f0;
    }}
    .bar-group {{
        flex: 1;
        display: flex;
        flex-direction: column;
        align-items: center;
        justify-content: flex-end;
        height: 100%;
        max-width: 60px;
    }}
    .bar {{
        width: 70%;
        border-radius: 6px 6px 0 0;
        min-height: 4px;
        transition: opacity 0.2s;
        position: relative;
        background: #94a3b8;
    }}
    .bar-ttft {{
        position: absolute;
        bottom: 0; left: 0; right: 0;
        background: rgba(15, 23, 42, 0.55);
        border-radius: 6px 6px 0 0;
    }}
    .bar:hover {{ opacity: 0.85; }}
    .chart-legend {{
        font-size: 0.75rem;
        font-weight: 400;
        color: #94a3b8;
        margin-left: 0.75rem;
    }}
    .bar-value {{
        font-size: 0.7rem;
        color: #64748b;
        margin-bottom: 0.25rem;
        white-space: nowrap;
    }}
    .bar-label {{
        font-size: 0.75rem;
        color: #94a3b8;
        margin-top: 0.4rem;
    }}
    footer {{
        text-align: center;
        color: #94a3b8;
        font-size: 0.8rem;
        margin-top: 1rem;
    }}
    .note {{
        font-size: 0.8rem;
        color: #64748b;
        margin-top: 0.75rem;
        line-height: 1.4;
    }}
</style>
</head>
<body>
<div class="container">
    <h1>Relatório de Teste Paralelo — Huawei MaaS</h1>
    <p class="subtitle">Gerado em {now} · {total_calls} chamadas em paralelo
        ({CALLS_PER_MODEL} por modelo) · Endpoint: {URL}</p>

    <div class="cards">{cards}
    </div>

    <div class="section">
        <h2>Métricas de latência e geração</h2>
        <table>
            <thead>
                <tr>
                    <th>Modelo</th>
                    <th>TTFT médio</th>
                    <th>TTFT P50 / P90</th>
                    <th>TPOT médio</th>
                    <th>Output tokens/sec</th>
                    <th>E2E Latency média</th>
                    <th>E2E P50 / P90 / P99</th>
                </tr>
            </thead>
            <tbody>{metrics_rows}
            </tbody>
        </table>
        <p class="note">TTFT = tempo até o primeiro token · TPOT = tempo médio entre tokens
            · E2E = latência total da requisição. Valores em ms quando &lt; 1s.</p>
    </div>

    <div class="section">
        <h2>Capacidade, confiabilidade e tokens</h2>
        <table>
            <thead>
                <tr>
                    <th>Modelo</th>
                    <th>QPS atingido</th>
                    <th>TPM atingido</th>
                    <th>Sucessos</th>
                    <th>Error rate</th>
                    <th>Tokens input médio</th>
                    <th>Tokens output médio</th>
                    <th>Tokens raciocínio médio</th>
                    <th>Total tokens</th>
                </tr>
            </thead>
            <tbody>{capacity_rows}
            </tbody>
        </table>
        <p class="note">QPS/TPM refletem a capacidade <strong>atingida neste teste</strong>
            (limitada pelo rate limit de {RATE_LIMIT_RPS} req/s do endpoint), não o limite do serviço.</p>
    </div>

    <div class="section">
        <h2>Latência por chamada (E2E com destaque de TTFT)</h2>
        {chart_bars}
    </div>

    <div class="section">
        <h2>Detalhamento das chamadas</h2>
        <table>
            <thead>
                <tr>
                    <th>Modelo</th>
                    <th>Chamada</th>
                    <th>Status</th>
                    <th>HTTP</th>
                    <th>TTFT</th>
                    <th>TPOT</th>
                    <th>Tok/s</th>
                    <th>E2E</th>
                    <th>In</th>
                    <th>Out</th>
                    <th>Raciocínio</th>
                    <th>Retries</th>
                </tr>
            </thead>
            <tbody>{detail_rows}
            </tbody>
        </table>
    </div>

    <footer>Relatório gerado automaticamente por maas_parallel_test.py</footer>
</div>
</body>
</html>"""


def main() -> None:
    if not API_KEY:
        raise SystemExit(
            "Erro: defina MAAS_API_KEY (ou API_KEY, por compatibilidade) antes de executar."
        )

    tasks = [(model, i) for model in MODELS for i in range(1, CALLS_PER_MODEL + 1)]

    print(f"Executando {len(tasks)} chamadas em paralelo "
          f"({CALLS_PER_MODEL} por modelo, modelos: {', '.join(MODELS)})...\n")

    results = []
    total_start = time.perf_counter()

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_task = {
            executor.submit(call_maas, model, call_id): (model, call_id)
            for model, call_id in tasks
        }

        for future in as_completed(future_to_task):
            result = future.result()
            results.append(result)
            status = "OK " if result["ok"] else "ERRO"
            retry_info = f" | retries: {result['retries']}" if result["retries"] else ""
            ttft_info = (f" | TTFT: {result['ttft']*1000:.0f}ms"
                         if result["ttft"] is not None else "")
            print(f"[{result['model']}] chamada {result['call_id']:02d} | "
                  f"{status} | HTTP {result['status']} | "
                  f"E2E: {result['latency']:.2f}s{ttft_info}{retry_info} | "
                  f"resposta: {(result['content'] or result['error'])[:60]}")

    total_elapsed = time.perf_counter() - total_start

    # -----------------------------------------------------------------------
    # Resumo por modelo
    # -----------------------------------------------------------------------
    print("\n" + "=" * 60)
    print("RESUMO")
    print("=" * 60)

    for model in MODELS:
        s = model_stats(results, model, total_elapsed)
        print(f"\nModelo: {model}")
        print(f"  Sucessos:        {s['ok']}/{s['total']} (error rate: {s['error_rate']:.1f}%)")
        print(f"  Retries totais:  {s['retries']}")
        print(f"  TTFT médio:      {s['ttft_mean']*1000:.0f}ms (P50: {s['ttft_p50']*1000:.0f}ms, P90: {s['ttft_p90']*1000:.0f}ms)")
        print(f"  TPOT médio:      {s['tpot_mean']*1000:.0f}ms")
        print(f"  Output tok/s:    {s['tokens_per_sec_mean']:.1f}")
        print(f"  E2E média:       {s['mean']:.2f}s (P50: {s['p50']:.2f}s, P90: {s['p90']:.2f}s, P99: {s['p99']:.2f}s)")
        print(f"  QPS atingido:    {s['qps']:.2f} | TPM atingido: {s['tpm']:.0f}")
        print(f"  Tokens médios:   in {s['avg_input_tokens']:.0f} / out {s['avg_output_tokens']:.0f}"
              f" (raciocínio: {s['avg_reasoning_tokens']:.0f})")

    print(f"\nTempo total de execução (todas em paralelo): {total_elapsed:.2f}s")

    # -----------------------------------------------------------------------
    # Gera relatório HTML
    # -----------------------------------------------------------------------
    html = build_html(results, total_elapsed)
    with open(REPORT_PATH, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"\nRelatório HTML gerado em: {REPORT_PATH}")


if __name__ == "__main__":
    main()
