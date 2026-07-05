"""Generate a representative synthetic sample for CI testing.
No dependency on the full 95MB CSV — creates realistic data from scratch."""
from __future__ import annotations

import csv
import logging
import os
import random
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)

PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_DEFAULT_DST = os.path.join(PROJECT_ROOT, "data", "sample_ci.csv")

SEED = int(os.environ.get("CI_SEED", "42"))
ROWS = 3000

random.seed(SEED)

countries = [
    "United Kingdom", "France", "Germany", "Australia", "Netherlands",
    "EIRE", "Spain", "Switzerland", "Belgium", "Portugal",
]
descriptions = [
    "WHITE HANGING HEART T-LIGHT HOLDER",
    "GREEN REGENCY TEACUP AND SAUCER",
    "PACK OF 72 RETROSPOT CAKE CASES",
    "JUMBO BAG RED RETROSPOT",
    "SET OF 3 BUTTERFLY COLOURING PENCILS",
    "POPCORN HOLDER",
    "LUNCH BAG VINTAGE DOILY",
    "SET OF 4 KNICK KNACK TINS",
    "VINTAGE UNION JACK BUNTING",
    "HAND WARMER OWL",
    "PACK OF 6 BUBBLE LIGHTS",
    "ROSES REGENCY TEACUP AND SAUCER",
    "SMALL POPCORN HOLDER",
    "LUNCH BAG SUKI DESIGN",
    "PAPER CHAIN KIT 50'S CHRISTMAS",
    "ASSORTED COLOUR BIRD ORNAMENT",
    "SET OF 3 HANGING OWLS",
    "PACK OF 6 SPACEBOY PRINTED T-LIGHTS",
    "SET/6 RED SPOTTY PAPCUPS",
    "SET/6 RED SPOTTY PAPER PLATES",
]
stock_codes = [
    "85123A", "71053", "84406B", "84029E", "22752",
    "21730", "22699", "23203", "21755", "84509A",
    "22492", "23437", "22728", "22377", "84380",
    "23254", "23271", "22384", "22469", "22197",
]


def random_customer_id() -> str:
    if random.random() < 0.23:
        return ""
    return f"{random.randint(12000, 20000)}"


def random_date() -> str:
    start = datetime(2010, 1, 1)
    end = datetime(2011, 12, 1)
    d = start + timedelta(seconds=random.randint(0, int((end - start).total_seconds())))
    return d.strftime("%Y-%m-%d %H:%M:%S")


def main() -> None:
    dst = os.environ.get("RAW_CSV_PATH", _DEFAULT_DST)
    rows = []
    for i in range(ROWS):
        is_return = random.random() < 0.022
        qty = random.randint(-50, -1) if is_return else random.randint(1, 50)
        price = round(random.uniform(0.5, 20.0) if not is_return else random.uniform(0.5, 15.0), 2)
        idx = random.randint(0, len(stock_codes) - 1)

        rows.append({
            "Invoice": f"{random.randint(530000, 590000)}",
            "StockCode": stock_codes[idx],
            "Description": descriptions[idx],
            "Quantity": str(qty),
            "InvoiceDate": random_date(),
            "Price": f"{price:.2f}",
            "Customer ID": random_customer_id(),
            "Country": random.choice(countries),
        })

    os.makedirs(os.path.dirname(dst), exist_ok=True)
    with open(dst, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=[
            "Invoice", "StockCode", "Description", "Quantity",
            "InvoiceDate", "Price", "Customer ID", "Country",
        ])
        writer.writeheader()
        writer.writerows(rows)

    actual_null = sum(1 for r in rows if not r["Customer ID"])
    actual_neg = sum(1 for r in rows if float(r["Quantity"]) < 0)
    logger.info("Wrote %d rows to %s", len(rows), dst)
    logger.info("  null customer_id: %d", actual_null)
    logger.info("  negative quantity: %d", actual_neg)


if __name__ == "__main__":
    main()
