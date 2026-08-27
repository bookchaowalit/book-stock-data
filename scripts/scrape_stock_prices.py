#!/usr/bin/env python3
"""Capture bounded Yahoo Finance chart data for the scheduler.

This repository is the collection producer. The separate ``book-finance-data``
repository owns lake-first ingestion and its read-only API; this adapter only
captures validated chart responses and writes local handoff artifacts.
"""

from __future__ import annotations

import csv
import json
import math
import re
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

try:
    import httpx
except ImportError as exc:  # pragma: no cover - requirements.txt supplies httpx
    raise RuntimeError("httpx is required for stock price capture") from exc


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "data" / "exported"
YAHOO_CHART_API = "https://query1.finance.yahoo.com/v8/finance/chart"
DEFAULT_SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "SPY", "QQQ"]
DEFAULT_RANGE = "5d"
DEFAULT_INTERVAL = "1d"
MAX_SYMBOLS = 50
ALLOWED_RANGES = {"5d", "1mo", "3mo", "6mo", "1y", "2y", "5y", "ytd"}
ALLOWED_INTERVALS = {"1d", "1wk", "1mo"}
PRICE_FIELDS = [
    "date",
    "symbol",
    "currency",
    "exchange",
    "price",
    "previous_close",
    "change_pct",
    "open",
    "high",
    "low",
    "volume",
    "interval",
    "updated_at",
]
HISTORY_FIELDS = ["date", "symbol", "currency", "price", "change_pct", "volume"]
_SYMBOL_RE = re.compile(r"^[A-Z0-9][A-Z0-9._=\-^]{0,11}$")


def _as_symbols(values: Iterable[str] | str | None) -> list[str]:
    if values is None:
        values = DEFAULT_SYMBOLS
    if isinstance(values, str):
        values = values.split(",")
    result: list[str] = []
    for value in values:
        symbol = str(value).strip().upper()
        if symbol and symbol not in result:
            result.append(symbol)
    return result


def normalize_request(
    symbols: Iterable[str] | str | None,
    interval: str = DEFAULT_INTERVAL,
    lookback: str = DEFAULT_RANGE,
) -> tuple[list[str], str, str]:
    """Normalize and bound symbols plus Yahoo chart request parameters."""

    normalized_symbols = _as_symbols(symbols)
    normalized_interval = str(interval).strip().lower()
    normalized_lookback = str(lookback).strip().lower()
    if not normalized_symbols or len(normalized_symbols) > MAX_SYMBOLS:
        raise ValueError(f"symbols must contain 1-{MAX_SYMBOLS} unique tickers")
    if any(not _SYMBOL_RE.fullmatch(symbol) for symbol in normalized_symbols):
        raise ValueError("symbols contain an invalid Yahoo Finance ticker")
    if normalized_interval not in ALLOWED_INTERVALS:
        raise ValueError(f"interval must be one of {sorted(ALLOWED_INTERVALS)}")
    if normalized_lookback not in ALLOWED_RANGES:
        raise ValueError(f"lookback must be one of {sorted(ALLOWED_RANGES)}")
    return normalized_symbols, normalized_interval, normalized_lookback


def _finite_number(value: Any, field: str, *, positive: bool = False) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)):
        raise ValueError(f"{field} must be numeric")
    number = float(value)
    if not math.isfinite(number) or (positive and number <= 0):
        suffix = " and greater than zero" if positive else ""
        raise ValueError(f"{field} must be finite{suffix}")
    return number


def _optional_number(values: list[Any], index: int, field: str) -> float | str:
    if index >= len(values) or values[index] is None:
        return ""
    return _finite_number(values[index], field)


def _timestamp(value: Any, field: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{field} must be a positive integer Unix timestamp")
    return value


def _latest_valid_index(timestamps: list[int], closes: list[Any], symbol: str) -> int:
    if len(closes) < len(timestamps):
        raise ValueError(f"{symbol} close series is shorter than timestamp series")
    for index in range(len(timestamps) - 1, -1, -1):
        value = closes[index]
        if value is None:
            continue
        _finite_number(value, f"{symbol}.close[{index}]", positive=True)
        return index
    raise ValueError(f"{symbol} has no valid close price")


def validate_payload(payload: Any, symbol: str, interval: str) -> dict[str, Any]:
    """Fail closed on malformed, partial, or timestamp-inconsistent responses."""

    if not isinstance(payload, dict):
        raise ValueError("Yahoo Finance response must be a JSON object")
    chart = payload.get("chart")
    if not isinstance(chart, dict):
        raise ValueError("Yahoo Finance response is missing chart object")
    if chart.get("error") not in (None, {}):
        raise ValueError(f"Yahoo Finance returned an error for {symbol}")
    results = chart.get("result")
    if not isinstance(results, list) or len(results) != 1 or not isinstance(results[0], dict):
        raise ValueError(f"Yahoo Finance response must contain one result for {symbol}")

    result = results[0]
    meta = result.get("meta")
    if not isinstance(meta, dict):
        raise ValueError(f"Yahoo Finance response is missing meta for {symbol}")
    response_symbol = str(meta.get("symbol") or "").strip().upper()
    if response_symbol != symbol:
        raise ValueError(f"Yahoo Finance symbol mismatch: expected {symbol}, got {response_symbol or 'missing'}")
    currency = str(meta.get("currency") or "").strip().upper()
    if not re.fullmatch(r"^[A-Z]{3,5}$", currency):
        raise ValueError(f"{symbol} currency is missing or invalid")
    granularity = str(meta.get("dataGranularity") or "").strip().lower()
    if granularity and granularity != interval:
        raise ValueError(f"{symbol} interval mismatch: expected {interval}, got {granularity}")

    timestamps_raw = result.get("timestamp")
    if not isinstance(timestamps_raw, list) or not timestamps_raw:
        raise ValueError(f"{symbol} timestamp series is required")
    timestamps = [_timestamp(value, f"{symbol}.timestamp[{index}]") for index, value in enumerate(timestamps_raw)]
    if any(left >= right for left, right in zip(timestamps, timestamps[1:])):
        raise ValueError(f"{symbol} timestamps must be strictly increasing")

    indicators = result.get("indicators")
    quote_series = indicators.get("quote") if isinstance(indicators, dict) else None
    if not isinstance(quote_series, list) or len(quote_series) != 1 or not isinstance(quote_series[0], dict):
        raise ValueError(f"Yahoo Finance quote series is missing for {symbol}")
    quote = quote_series[0]
    closes = quote.get("close")
    if not isinstance(closes, list):
        raise ValueError(f"{symbol} close series is required")
    index = _latest_valid_index(timestamps, closes, symbol)
    price = _finite_number(closes[index], f"{symbol}.close[{index}]", positive=True)

    previous_raw = meta.get("chartPreviousClose")
    if previous_raw is not None:
        previous_close = _finite_number(previous_raw, f"{symbol}.chartPreviousClose", positive=True)
    else:
        previous_close = None
        for previous_index in range(index - 1, -1, -1):
            if closes[previous_index] is not None:
                previous_close = _finite_number(closes[previous_index], f"{symbol}.close[{previous_index}]", positive=True)
                break
    if previous_close is None:
        raise ValueError(f"{symbol} previous close is unavailable")

    change_pct = round(((price - previous_close) / previous_close) * 100, 3)
    if not math.isfinite(change_pct):
        raise ValueError(f"{symbol} change_pct is not finite")
    date = datetime.fromtimestamp(timestamps[index], timezone.utc).date().isoformat()

    def series(name: str) -> list[Any]:
        values = quote.get(name)
        return values if isinstance(values, list) else []

    volumes = series("volume")
    volume = _optional_number(volumes, index, f"{symbol}.volume")
    if volume != "":
        volume = int(volume)
    return {
        "date": date,
        "timestamp": timestamps[index],
        "symbol": symbol,
        "currency": currency,
        "exchange": str(meta.get("fullExchangeName") or meta.get("exchangeName") or "").strip(),
        "price": price,
        "previous_close": previous_close,
        "change_pct": change_pct,
        "open": _optional_number(series("open"), index, f"{symbol}.open"),
        "high": _optional_number(series("high"), index, f"{symbol}.high"),
        "low": _optional_number(series("low"), index, f"{symbol}.low"),
        "volume": volume,
        "interval": interval,
    }


def fetch_quotes(
    symbols: list[str], interval: str, lookback: str
) -> tuple[bytes, list[dict[str, Any]]]:
    """Fetch one bounded chart response per ticker and validate every result."""

    raw_payloads: dict[str, Any] = {}
    rows: list[dict[str, Any]] = []
    for symbol in symbols:
        response = httpx.get(
            f"{YAHOO_CHART_API}/{symbol}",
            params={"range": lookback, "interval": interval, "events": "div,splits"},
            headers={"User-Agent": "book-job-scraping/1.0"},
            timeout=30,
        )
        response.raise_for_status()
        payload = response.json()
        raw_payloads[symbol] = payload
        rows.append(validate_payload(payload, symbol, interval))
    raw = json.dumps(raw_payloads, ensure_ascii=False, separators=(",", ":")).encode("utf-8")
    return raw, rows


def _utc_now() -> str:
    return datetime.now(timezone.utc).replace(microsecond=0).isoformat().replace("+00:00", "Z")


def price_rows(rows: list[dict[str, Any]], observed_at: str) -> list[dict[str, Any]]:
    return [
        {
            **row,
            "updated_at": observed_at,
        }
        for row in rows
    ]


def write_raw(raw: bytes, output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "stock_prices_raw.json"
    path.write_bytes(raw)
    return path


def write_snapshot(rows: list[dict[str, Any]], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "stock_prices.csv"
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=PRICE_FIELDS, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    return path


def append_history(rows: list[dict[str, Any]], output_dir: Path) -> Path:
    output_dir.mkdir(parents=True, exist_ok=True)
    path = output_dir / "stock_history.csv"
    exists = path.exists()
    with path.open("a", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=HISTORY_FIELDS, extrasaction="ignore")
        if not exists:
            writer.writeheader()
        for row in rows:
            writer.writerow({field: row[field] for field in HISTORY_FIELDS})
    return path


def print_alerts(rows: list[dict[str, Any]], threshold: float) -> list[dict[str, Any]]:
    alerts = [row for row in rows if abs(float(row["change_pct"])) >= threshold]
    for row in sorted(alerts, key=lambda item: abs(float(item["change_pct"])), reverse=True):
        direction = "UP" if float(row["change_pct"]) > 0 else "DOWN"
        print(f"  ALERT {direction}: {row['symbol']} {row['change_pct']:+.3f}% (${row['price']:,.2f})")
    return alerts


class StockPriceScraper:
    """Scheduler adapter for bounded Yahoo Finance chart capture."""

    def __init__(
        self,
        symbols: Iterable[str] | str | None = None,
        interval: str = DEFAULT_INTERVAL,
        range: str = DEFAULT_RANGE,
        alert_threshold: float = 3.0,
        output_dir: str | Path | None = None,
        **_: Any,
    ) -> None:
        self.symbols, self.interval, self.lookback = normalize_request(symbols, interval, range)
        self.alert_threshold = float(alert_threshold)
        if self.alert_threshold < 0:
            raise ValueError("alert_threshold must be non-negative")
        self.output_dir = Path(output_dir) if output_dir else OUTPUT_DIR

    async def run(self, **_: Any) -> list[dict[str, Any]]:
        raw, validated_rows = fetch_quotes(self.symbols, self.interval, self.lookback)
        observed_at = _utc_now()
        rows = price_rows(validated_rows, observed_at)
        raw_path = write_raw(raw, self.output_dir)
        snapshot_path = write_snapshot(rows, self.output_dir)
        history_path = append_history(rows, self.output_dir)
        alerts = print_alerts(rows, self.alert_threshold)
        print(f"[stock_prices] {len(rows)} rows -> {snapshot_path}")
        return [
            {
                "source": "stock_prices",
                "count": len(rows),
                "alerts": len(alerts),
                "output": str(snapshot_path),
                "history": str(history_path),
                "raw": str(raw_path),
            }
        ]


if __name__ == "__main__":
    import asyncio

    asyncio.run(StockPriceScraper().run())
