#!/usr/bin/env python3
"""Run bounded Yahoo Finance capture owned by this repository."""

from __future__ import annotations

import argparse
import asyncio
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "scripts"))

from scrape_stock_prices import StockPriceScraper

SYMBOLS = ["AAPL", "MSFT", "GOOGL", "AMZN", "NVDA", "META", "TSLA", "SPY", "QQQ"]


async def run_stock(output_dir: Path) -> list[dict[str, Any]]:
    scraper = StockPriceScraper(
        symbols=SYMBOLS,
        range="5d",
        interval="1d",
        alert_threshold=3.0,
        output_dir=output_dir,
    )
    batch = await scraper.run()
    print(f"[run_stock] stock_prices: {batch[0].get('count') if batch else 0}")
    return batch


def main() -> int:
    parser = argparse.ArgumentParser(description="Run book-stock-data Yahoo Finance collection")
    parser.add_argument("--output-dir", type=Path, default=ROOT / "data" / "exported")
    parser.add_argument("--json", action="store_true")
    args = parser.parse_args()
    results = asyncio.run(run_stock(args.output_dir))
    if args.json:
        print(json.dumps(results, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
