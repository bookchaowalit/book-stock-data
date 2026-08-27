from pathlib import Path


def test_collection_scripts_exist():
    root = Path(__file__).resolve().parents[1]
    assert (root / "scripts" / "run_stock.py").is_file()
    assert (root / "scripts" / "scrape_stock_prices.py").is_file()
    assert (root / "setup_cron.sh").is_file()
