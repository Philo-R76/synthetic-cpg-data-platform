"""Generate deterministic, messy CPG source feeds for the ADP portfolio demo."""

from __future__ import annotations

import argparse
import calendar
import csv
import json
import random
from datetime import date, timedelta
from pathlib import Path
from typing import Any

from src.ontology import DEFAULT_ONTOLOGY_PATH, load_ontology


TENANTS = (
    {"tenant_id": "Tenant_A", "currency": "USD", "market": "US"},
    {"tenant_id": "Tenant_B", "currency": "CAD", "market": "CA"},
    {"tenant_id": "Tenant_C", "currency": "EUR", "market": "DE"},
)

PRODUCTS = (
    {
        "product_id": "SKU-1001",
        "canonical_sku": "NS-OAT-1L",
        "brand": "Northstar",
        "category": "Plant Drinks",
        "product_name": "Oat Drink Original",
        "pack_size": "1",
        "pack_unit": "L",
        "price": 3.49,
    },
    {
        "product_id": "SKU-1002",
        "canonical_sku": "NS-OAT-6X250",
        "brand": "Northstar",
        "category": "Plant-Based Beverages",
        "product_name": "Oat Drink Barista Multipack",
        "pack_size": "6x250",
        "pack_unit": "ml",
        "price": 5.99,
    },
    {
        "product_id": "SKU-2001",
        "canonical_sku": "CR-CHOC-150",
        "brand": "Crisp Ridge",
        "category": "Snacks",
        "product_name": "Chocolate Grain Bites",
        "pack_size": "150",
        "pack_unit": "g",
        "price": 2.79,
    },
    {
        "product_id": "SKU-2002",
        "canonical_sku": "CR-SALT-200",
        "brand": "Crisp Ridge",
        "category": "Snacks",
        "product_name": "Sea Salt Crisps",
        "pack_size": "200",
        "pack_unit": "g",
        "price": 3.19,
    },
    {
        "product_id": "SKU-3001",
        "canonical_sku": "GB-LAUN-1.5",
        "brand": "Green Basket",
        "category": "Household Care",
        "product_name": "Plant Laundry Liquid",
        "pack_size": "1.5",
        "pack_unit": "L",
        "price": 8.49,
    },
    {
        "product_id": "SKU-3002",
        "canonical_sku": "GB-DISH-500",
        "brand": "Green Basket",
        "category": "Household Care",
        "product_name": "Citrus Dish Soap",
        "pack_size": "500",
        "pack_unit": "ml",
        "price": 3.79,
    },
    {
        "product_id": "SKU-4001",
        "canonical_sku": "SP-SHAM-400",
        "brand": "Soft Peak",
        "category": "Personal Care",
        "product_name": "Daily Moisture Shampoo",
        "pack_size": "400",
        "pack_unit": "ml",
        "price": 6.49,
    },
    {
        "product_id": "SKU-4002",
        "canonical_sku": "SP-COND-400",
        "brand": "Soft Peak",
        "category": "Personal Care",
        "product_name": "Daily Moisture Conditioner",
        "pack_size": "400",
        "pack_unit": "ml",
        "price": 6.49,
    },
)

FEED_FIELDS = {
    "pos.csv": (
        "record_id", "tenant_id", "source_system", "transaction_date",
        "store_id", "retailer_sku", "units_sold", "net_sales", "currency",
        "sales_channel",
    ),
    "shipments.csv": (
        "record_id", "tenant_id", "source_system", "ship_date",
        "warehouse_id", "retailer_id", "item_number", "shipped_quantity",
        "shipment_value", "currency", "unit_of_measure",
    ),
    "promotions.csv": (
        "record_id", "tenant_id", "source_system", "promotion_id",
        "retailer_sku", "start_date", "end_date", "discount_pct",
        "promo_spend", "currency",
    ),
    "syndicated.csv": (
        "record_id", "tenant_id", "source_system", "provider", "market",
        "period", "product_code", "market_units", "market_sales",
        "currency", "unit_of_measure",
    ),
}

PRODUCT_FIELDS = (
    "product_id", "canonical_sku", "brand", "category", "product_name",
    "pack_size", "pack_unit",
)

PRODUCT_HISTORY_FIELDS = PRODUCT_FIELDS + (
    "effective_from",
    "effective_to",
    "is_current",
)

LABEL_FIELDS = (
    "record_id", "tenant_id", "source_system", "raw_product_value",
    "expected_product_id", "expected_brand", "expected_category",
    "label_status",
)


def _month_start(month_offset: int) -> date:
    year = 2025 + month_offset // 12
    month = month_offset % 12 + 1
    return date(year, month, 1)


def _currency(tenant_currency: str, rng: random.Random) -> str:
    variants = {
        "USD": ("USD", "US$", "$"),
        "CAD": ("CAD", "C$", "$"),
        "EUR": ("EUR", "€"),
    }
    return rng.choice(variants[tenant_currency])


def _raw_product(
    product: dict[str, Any],
    rng: random.Random,
    ontology: dict[str, Any],
    prefer_synonym: bool = False,
    prefer_fuzzy: bool = False,
) -> str:
    synonyms = ontology["entities"][product["product_id"]]["synonyms"]
    aliases = (
        product["canonical_sku"],
        product["product_name"],
        f'{product["brand"]} {product["product_name"]}',
        f'{product["brand"]} {product["product_name"]} {product["pack_size"]}{product["pack_unit"]}',
        *synonyms,
    )
    if prefer_fuzzy and synonyms:
        value = rng.choice(synonyms)
        alphabetic_positions = [
            index
            for index in range(1, len(value) - 1)
            if value[index].isalpha()
        ]
        if alphabetic_positions:
            typo_index = alphabetic_positions[len(alphabetic_positions) // 2]
            value = value[:typo_index] + value[typo_index + 1 :]
    elif prefer_synonym and synonyms:
        value = rng.choice(synonyms)
    else:
        value = rng.choice(aliases)
    if rng.random() < 0.2:
        value = f" {value.lower()} "
    if rng.random() < 0.1:
        value = value.replace("-", " ")
    return value


def _make_label(
    record_id: str,
    tenant_id: str,
    source_system: str,
    raw_product_value: str,
    product: dict[str, Any],
) -> dict[str, str]:
    return {
        "record_id": record_id,
        "tenant_id": tenant_id,
        "source_system": source_system,
        "raw_product_value": raw_product_value,
        "expected_product_id": product["product_id"],
        "expected_brand": product["brand"],
        "expected_category": product["category"],
        "label_status": "missing_raw_value" if not raw_product_value else "labeled",
    }


def _write_csv(path: Path, fields: tuple[str, ...], rows: list[dict[str, Any]]) -> None:
    with path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=fields, extrasaction="raise")
        writer.writeheader()
        writer.writerows(rows)


def generate_dataset(
    output_dir: Path,
    seed: int = 42,
    months: int = 12,
    records_per_month: int = 24,
    ontology_path: Path = DEFAULT_ONTOLOGY_PATH,
) -> dict[str, Any]:
    """Write all synthetic feeds, catalog, labels, dictionary, and manifest."""
    if months < 1:
        raise ValueError("months must be at least 1")
    if records_per_month < 1:
        raise ValueError("records_per_month must be at least 1")

    output_dir.mkdir(parents=True, exist_ok=True)
    rng = random.Random(seed)
    ontology = load_ontology(ontology_path)
    if set(ontology["entities"]) != {product["product_id"] for product in PRODUCTS}:
        raise ValueError("Ontology product IDs must exactly match the synthetic catalog")
    rows_by_file: dict[str, list[dict[str, Any]]] = {
        filename: [] for filename in FEED_FIELDS
    }
    labels: list[dict[str, str]] = []
    anomalies = {
        "invalid_dates": 0,
        "missing_product_values": 0,
        "invalid_numeric_values": 0,
    }

    for tenant in TENANTS:
        tenant_id = tenant["tenant_id"]
        for month_offset in range(months):
            month = _month_start(month_offset)
            days_in_month = calendar.monthrange(month.year, month.month)[1]
            for index in range(records_per_month):
                product = rng.choice(PRODUCTS)
                raw_product = _raw_product(
                    product,
                    rng,
                    ontology,
                    prefer_synonym=index == records_per_month - 1,
                    prefer_fuzzy=(
                        records_per_month >= 4
                        and index == records_per_month - 2
                    ),
                )
                if index == 1:
                    raw_product = ""
                    anomalies["missing_product_values"] += 4

                date_value = month + timedelta(days=rng.randrange(days_in_month))
                date_text = date_value.isoformat()
                if index == 0:
                    date_text = "not-a-date"
                    anomalies["invalid_dates"] += 2

                currency = _currency(tenant["currency"], rng)
                row_suffix = f"{month:%Y%m}-{index + 1:04d}"

                record_id = f"pos-{tenant_id}-{row_suffix}"
                units = rng.randint(1, 24)
                units_text = "many" if index == 2 else str(units)
                if index == 2:
                    anomalies["invalid_numeric_values"] += 1
                rows_by_file["pos.csv"].append({
                    "record_id": record_id,
                    "tenant_id": tenant_id,
                    "source_system": "retailer_pos",
                    "transaction_date": date_text,
                    "store_id": f"STORE-{rng.randint(1, 80):03d}",
                    "retailer_sku": raw_product,
                    "units_sold": units_text,
                    "net_sales": f'{units * product["price"]:.2f}',
                    "currency": currency,
                    "sales_channel": rng.choice(("grocery", "mass", "online")),
                })
                labels.append(_make_label(record_id, tenant_id, "retailer_pos", raw_product, product))

                record_id = f"ship-{tenant_id}-{row_suffix}"
                quantity = rng.randint(10, 160)
                quantity_text = "unknown" if index == 2 else str(quantity)
                if index == 2:
                    anomalies["invalid_numeric_values"] += 1
                rows_by_file["shipments.csv"].append({
                    "record_id": record_id,
                    "tenant_id": tenant_id,
                    "source_system": "warehouse",
                    "ship_date": date_text,
                    "warehouse_id": f"WH-{rng.randint(1, 5):02d}",
                    "retailer_id": f"RET-{rng.randint(1, 25):03d}",
                    "item_number": raw_product,
                    "shipped_quantity": quantity_text,
                    "shipment_value": f'{quantity * product["price"] * 0.72:.2f}',
                    "currency": currency,
                    "unit_of_measure": rng.choice(("EA", "each", "CASE", "CS")),
                })
                labels.append(_make_label(record_id, tenant_id, "warehouse", raw_product, product))

                record_id = f"promo-{tenant_id}-{row_suffix}"
                promo_start = month + timedelta(days=rng.randrange(min(28, days_in_month)))
                promo_end = promo_start + timedelta(days=rng.randint(7, 35))
                promo_date_text = "not-a-date" if index == 0 else promo_start.isoformat()
                if index == 0:
                    anomalies["invalid_dates"] += 1
                rows_by_file["promotions.csv"].append({
                    "record_id": record_id,
                    "tenant_id": tenant_id,
                    "source_system": "trade_promotion",
                    "promotion_id": f"PR-{tenant_id}-{row_suffix}",
                    "retailer_sku": raw_product,
                    "start_date": promo_date_text,
                    "end_date": promo_end.isoformat(),
                    "discount_pct": f"{rng.randint(5, 30) / 100:.2f}",
                    "promo_spend": f"{rng.uniform(100, 2500):.2f}",
                    "currency": currency,
                })
                labels.append(_make_label(record_id, tenant_id, "trade_promotion", raw_product, product))

                record_id = f"synd-{tenant_id}-{row_suffix}"
                market_units = rng.randint(100, 2500)
                rows_by_file["syndicated.csv"].append({
                    "record_id": record_id,
                    "tenant_id": tenant_id,
                    "source_system": "syndicated",
                    "provider": rng.choice(("PanelOne", "MarketScope")),
                    "market": tenant["market"],
                    "period": month.strftime("%Y-%m"),
                    "product_code": raw_product,
                    "market_units": "n/a" if index == 2 else str(market_units),
                    "market_sales": f'{market_units * product["price"]:.2f}',
                    "currency": currency,
                    "unit_of_measure": rng.choice(("units", "EA", "cases")),
                })
                if index == 2:
                    anomalies["invalid_numeric_values"] += 1
                labels.append(_make_label(record_id, tenant_id, "syndicated", raw_product, product))

    for filename, rows in rows_by_file.items():
        _write_csv(output_dir / filename, FEED_FIELDS[filename], rows)
    _write_csv(
        output_dir / "product_catalog.csv",
        PRODUCT_FIELDS,
        [{key: product[key] for key in PRODUCT_FIELDS} for product in PRODUCTS],
    )
    product_history_rows: list[dict[str, Any]] = []
    for product in PRODUCTS:
        current = {key: product[key] for key in PRODUCT_FIELDS}
        if product["product_id"] == "SKU-1001":
            previous = {**current, "category": "Plant-Based Beverages"}
            product_history_rows.append({
                **previous,
                "effective_from": "1900-01-01",
                "effective_to": "2025-07-01",
                "is_current": "false",
            })
            product_history_rows.append({
                **current,
                "effective_from": "2025-07-01",
                "effective_to": "",
                "is_current": "true",
            })
        else:
            product_history_rows.append({
                **current,
                "effective_from": "1900-01-01",
                "effective_to": "",
                "is_current": "true",
            })
    _write_csv(
        output_dir / "product_catalog_history.csv",
        PRODUCT_HISTORY_FIELDS,
        product_history_rows,
    )
    _write_csv(output_dir / "mapping_ground_truth.csv", LABEL_FIELDS, labels)

    dictionary = {
        "description": "Synthetic CPG data for local ingestion, mapping, and quality demos.",
        "period": f"{_month_start(0):%Y-%m} through {_month_start(months - 1):%Y-%m}",
        "tenants": [tenant["tenant_id"] for tenant in TENANTS],
        "files": {
            filename: {
                "rows": len(rows),
                "fields": list(FEED_FIELDS[filename]),
            }
            for filename, rows in rows_by_file.items()
        },
        "product_catalog_history_rows": len(product_history_rows),
        "field_notes": {
            "transaction_date / ship_date / start_date / end_date": "ISO date when valid; selected rows contain not-a-date.",
            "currency": "Tenant currency with realistic symbol/code variations.",
            "units_sold / shipped_quantity / market_units": "Raw numeric strings; selected rows contain invalid values.",
            "discount_pct": "Fraction from 0.05 to 0.30, not a whole-number percentage.",
            "mapping_ground_truth.csv": "Synthetic row-level labels; expected product is retained even when the raw product value is missing.",
            "retailer_sku / item_number / product_code": "Includes canonical, descriptive, and ontology-synonym product values.",
            "product_catalog_history.csv": "Contains an intentional SKU-1001 category change effective 2025-07-01 for visible SCD Type 2 history.",
        },
        "injected_anomalies": anomalies,
        "note": "All data and benchmark labels are synthetic and do not represent real customers or measured production performance.",
    }
    (output_dir / "data_dictionary.json").write_text(
        json.dumps(dictionary, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )

    manifest = {
        "seed": seed,
        "months": months,
        "records_per_month_per_tenant_per_feed": records_per_month,
        "tenant_count": len(TENANTS),
        "feed_row_counts": {filename: len(rows) for filename, rows in rows_by_file.items()},
        "product_count": len(PRODUCTS),
        "ground_truth_label_count": len(labels),
        "injected_anomalies": anomalies,
    }
    (output_dir / "generation_manifest.json").write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    return manifest


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--months", type=int, default=12)
    parser.add_argument("--records-per-month", type=int, default=24)
    parser.add_argument("--ontology", type=Path, default=DEFAULT_ONTOLOGY_PATH)
    args = parser.parse_args()

    manifest = generate_dataset(
        args.output_dir,
        seed=args.seed,
        months=args.months,
        records_per_month=args.records_per_month,
        ontology_path=args.ontology,
    )
    print(json.dumps(manifest, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
