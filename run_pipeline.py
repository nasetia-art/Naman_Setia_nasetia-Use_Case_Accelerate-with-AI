import argparse
import csv
import shutil
from collections import defaultdict
from datetime import datetime
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from pathlib import Path


ALIASES = {
    "transaction_id": {"transactionid", "txnid", "saleid"},
    "transaction_date": {"transactiondate", "date", "saledate"},
    "store_id": {"storeid", "store"},
    "product_id": {"productid", "sku", "itemid"},
    "quantity": {"quantity", "qty", "units"},
    "unit_price": {"unitprice", "price", "salesprice"},
    "discount": {"discount", "discountpct", "discountpercent"},
}
REQUIRED = {
    "transaction_id", "transaction_date", "store_id",
    "product_id", "quantity", "unit_price",
}


def normalize_header(header):
    return "".join(char.lower() for char in header if char.isalnum())


def parse_date(value):
    for date_format in (
        "%Y-%m-%d", "%m/%d/%Y", "%d/%m/%Y",
        "%Y/%m/%d", "%b %d %Y",
    ):
        try:
            return datetime.strptime(value.strip(), date_format).date().isoformat()
        except ValueError:
            pass
    raise ValueError("invalid transaction date")


def parse_decimal(value):
    return Decimal(value.strip().replace("$", "").replace(",", ""))


def as_money(value):
    return value.quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)


def write_csv(path, rows, columns):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=columns)
        writer.writeheader()
        writer.writerows(rows)


def ingest_agent(raw_dir, bronze_dir):
    """Copy source CSVs to Bronze and read their rows."""
    if not raw_dir.is_dir():
        raise FileNotFoundError(f"Raw data directory not found: {raw_dir}")

    bronze_dir.mkdir(parents=True, exist_ok=True)
    source_rows = []

    for source_file in sorted(raw_dir.glob("*.csv")):
        shutil.copy2(source_file, bronze_dir / source_file.name)

        with source_file.open("r", newline="", encoding="utf-8-sig") as file:
            for row_number, row in enumerate(csv.DictReader(file), start=2):
                source_rows.append({
                    "file": source_file.name,
                    "row_number": row_number,
                    "raw": {
                        normalize_header(key): value or ""
                        for key, value in row.items()
                        if key
                    },
                })

    return source_rows


def cleanse_agent(source_rows, silver_dir):
    """Standardize fields, validate records, and save rejected rows."""
    clean_rows = []
    rejected_rows = []
    seen_ids = set()

    for source in source_rows:
        raw = source["raw"]
        record = {}

        for field, names in ALIASES.items():
            for name in names:
                if name in raw:
                    record[field] = raw[name].strip()
                    break

        reason = None
        missing = REQUIRED - record.keys()
        if missing:
            reason = "missing fields: " + ", ".join(sorted(missing))

        try:
            if reason:
                raise ValueError(reason)

            record["transaction_date"] = parse_date(record["transaction_date"])
            quantity = parse_decimal(record["quantity"])
            unit_price = parse_decimal(record["unit_price"])

            discount_text = record.get("discount", "0").strip()
            discount = parse_decimal(discount_text.rstrip("%")) if discount_text else Decimal("0")
            if discount_text.endswith("%"):
                discount /= 100

            if quantity < 0:
                raise ValueError("quantity cannot be negative")
            if unit_price < 0:
                raise ValueError("unit price cannot be negative")
            if not 0 <= discount <= 1:
                raise ValueError("discount must be between 0 and 1")

            transaction_id = record["transaction_id"]
            if transaction_id in seen_ids:
                raise ValueError("duplicate transaction ID")

            seen_ids.add(transaction_id)
            clean_rows.append({
                "transaction_id": transaction_id,
                "transaction_date": record["transaction_date"],
                "store_id": record["store_id"],
                "product_id": record["product_id"],
                "quantity": str(quantity),
                "unit_price": str(as_money(unit_price)),
                "discount": str(discount),
                "net_sales": str(as_money(quantity * unit_price * (1 - discount))),
            })

        except (ValueError, InvalidOperation) as error:
            rejected_rows.append({
                "source_file": source["file"],
                "source_row": source["row_number"],
                "reason": str(error),
            })

    write_csv(
        silver_dir / "sales_silver.csv",
        clean_rows,
        [
            "transaction_id", "transaction_date", "store_id", "product_id",
            "quantity", "unit_price", "discount", "net_sales",
        ],
    )
    write_csv(
        silver_dir / "rejected_rows.csv",
        rejected_rows,
        ["source_file", "source_row", "reason"],
    )

    return clean_rows, rejected_rows


def aggregate_agent(silver_rows, gold_dir):
    """Create daily sales totals for each store."""
    totals = defaultdict(lambda: {
        "gross_sales": Decimal("0"),
        "net_sales": Decimal("0"),
        "units_sold": Decimal("0"),
        "transaction_count": 0,
    })

    for row in silver_rows:
        key = (row["transaction_date"], row["store_id"])
        total = totals[key]
        quantity = Decimal(row["quantity"])

        total["gross_sales"] += quantity * Decimal(row["unit_price"])
        total["net_sales"] += Decimal(row["net_sales"])
        total["units_sold"] += quantity
        total["transaction_count"] += 1

    gold_rows = []
    for (date, store_id), total in sorted(totals.items()):
        gold_rows.append({
            "transaction_date": date,
            "store_id": store_id,
            "gross_sales": str(as_money(total["gross_sales"])),
            "net_sales": str(as_money(total["net_sales"])),
            "units_sold": str(total["units_sold"]),
            "transaction_count": total["transaction_count"],
        })

    write_csv(
        gold_dir / "daily_store_sales.csv",
        gold_rows,
        [
            "transaction_date", "store_id", "gross_sales",
            "net_sales", "units_sold", "transaction_count",
        ],
    )
    return gold_rows


def reporting_agent(silver_rows, rejected_rows, gold_rows, reports_dir):
    """Write a concise Markdown summary of the pipeline results."""
    net_sales = sum(
        (Decimal(row["net_sales"]) for row in silver_rows),
        Decimal("0"),
    )

    report = [
        "# Retail Sales Report",
        "",
        f"- Valid transactions: {len(silver_rows)}",
        f"- Rejected rows: {len(rejected_rows)}",
        f"- Daily store groups: {len(gold_rows)}",
        f"- Total net sales: ${as_money(net_sales)}",
        "",
        "## Daily sales by store",
        "",
        "| Date | Store | Net sales | Transactions |",
        "|---|---|---:|---:|",
    ]

    for row in gold_rows:
        report.append(
            f"| {row['transaction_date']} | {row['store_id']} "
            f"| ${row['net_sales']} | {row['transaction_count']} |"
        )

    reports_dir.mkdir(parents=True, exist_ok=True)
    (reports_dir / "sales_report.md").write_text(
        "\n".join(report) + "\n",
        encoding="utf-8",
    )


def main():
    parser = argparse.ArgumentParser(description="Retail sales medallion workflow")
    parser.add_argument("--raw", type=Path, default=Path("data/raw"))
    parser.add_argument("--bronze", type=Path, default=Path("data/bronze"))
    parser.add_argument("--silver", type=Path, default=Path("data/silver"))
    parser.add_argument("--gold", type=Path, default=Path("data/gold"))
    parser.add_argument("--reports", type=Path, default=Path("reports"))
    args = parser.parse_args()

    bronze_rows = ingest_agent(args.raw, args.bronze)
    silver_rows, rejected_rows = cleanse_agent(bronze_rows, args.silver)
    gold_rows = aggregate_agent(silver_rows, args.gold)
    reporting_agent(silver_rows, rejected_rows, gold_rows, args.reports)

    print(
        f"Done: {len(silver_rows)} valid rows, "
        f"{len(rejected_rows)} rejected rows."
    )


if __name__ == "__main__":
    main()