"""Load generated CPG feeds into DuckDB and run a measurable mapping benchmark."""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from src.ontology import (
    DEFAULT_ONTOLOGY_PATH,
    build_alias_index,
    load_ontology,
    normalize_product_value,
    resolve_product,
)


FEED_TABLES = {
    "pos.csv": "bronze.pos_raw",
    "shipments.csv": "bronze.shipments_raw",
    "promotions.csv": "bronze.promotions_raw",
    "syndicated.csv": "bronze.syndicated_raw",
}

def _load_csv(
    connection: duckdb.DuckDBPyConnection,
    table: str,
    path: Path,
) -> None:
    connection.execute(f"DELETE FROM {table}")
    connection.execute(
        f"INSERT INTO {table} SELECT * FROM read_csv(?, header=true, all_varchar=true)",
        [str(path)],
    )


def _quote_identifier(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _ingest_feed(
    connection: duckdb.DuckDBPyConnection,
    feed_name: str,
    table: str,
    path: Path,
    run_id: str,
    force_refresh: bool,
) -> dict[str, Any]:
    content_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    previous = connection.execute(
        """
        SELECT content_sha256, source_row_count
        FROM platform.feed_ingestion_state
        WHERE feed_name = ?
        """,
        [feed_name],
    ).fetchone()
    loaded_at = datetime.now(timezone.utc)
    if previous is not None and previous[0] == content_hash and not force_refresh:
        source_rows = previous[1]
        inserted_rows = 0
        updated_rows = 0
        skipped_unchanged = True
    else:
        stage_name = "_feed_stage"
        stage = _quote_identifier(stage_name)
        connection.execute(
            f"CREATE OR REPLACE TEMP TABLE {stage} AS "
            "SELECT * FROM read_csv(?, header=true, all_varchar=true)",
            [str(path)],
        )
        target_columns = [
            row[0] for row in connection.execute(f"DESCRIBE {table}").fetchall()
        ]
        stage_columns = [
            row[0] for row in connection.execute(f"DESCRIBE {stage}").fetchall()
        ]
        if set(stage_columns) != set(target_columns):
            missing = sorted(set(target_columns) - set(stage_columns))
            unexpected = sorted(set(stage_columns) - set(target_columns))
            raise ValueError(
                f"Schema drift in {feed_name}: missing columns {missing}; "
                f"unexpected columns {unexpected}"
            )
        invalid_key = connection.execute(
            f"""
            SELECT record_id, count(*) AS occurrences
            FROM {stage}
            WHERE record_id IS NULL OR trim(record_id) = ''
            GROUP BY record_id
            LIMIT 1
            """
        ).fetchone()
        if invalid_key is not None:
            raise ValueError(f"Missing record_id in feed {feed_name}")
        duplicate = connection.execute(
            f"""
            SELECT record_id, count(*) AS occurrences
            FROM {stage}
            GROUP BY record_id
            HAVING count(*) > 1
            LIMIT 1
            """
        ).fetchone()
        if duplicate is not None:
            raise ValueError(
                f"Duplicate record_id {duplicate[0]!r} in feed {feed_name}"
            )

        source_rows = connection.execute(f"SELECT count(*) FROM {stage}").fetchone()[0]
        fields = [column for column in target_columns if column != "record_id"]
        differences = " OR ".join(
            f"target.{_quote_identifier(field)} IS DISTINCT FROM "
            f"source.{_quote_identifier(field)}"
            for field in fields
        )
        inserted_rows = connection.execute(
            f"""
            SELECT count(*)
            FROM {stage} AS source
            LEFT JOIN {table} AS target USING (record_id)
            WHERE target.record_id IS NULL
            """
        ).fetchone()[0]
        updated_rows = connection.execute(
            f"""
            SELECT count(*)
            FROM {stage} AS source
            JOIN {table} AS target USING (record_id)
            WHERE {differences}
            """
        ).fetchone()[0]
        changed_record_rows = connection.execute(
            f"""
            SELECT source.record_id,
                   CASE WHEN target.record_id IS NULL THEN 'INSERT'
                        WHEN {str(force_refresh).lower()} THEN 'REFRESH'
                        ELSE 'UPDATE' END AS change_type
            FROM {stage} AS source
            LEFT JOIN {table} AS target USING (record_id)
            WHERE target.record_id IS NULL
               OR ({differences})
               OR {str(force_refresh).lower()}
            """
        ).fetchall()
        if changed_record_rows:
            connection.executemany(
                "INSERT INTO platform.feed_ingestion_changes VALUES (?, ?, ?, ?)",
                [
                    (run_id, feed_name, record_id, change_type)
                    for record_id, change_type in changed_record_rows
                ],
            )
        target_list = ", ".join(_quote_identifier(column) for column in target_columns)
        source_list = ", ".join(
            f"source.{_quote_identifier(column)}" for column in target_columns
        )
        update_list = ", ".join(
            f"{_quote_identifier(field)} = source.{_quote_identifier(field)}"
            for field in fields
        )
        connection.execute(
            f"""
            MERGE INTO {table} AS target
            USING {stage} AS source
               ON target.record_id = source.record_id
            WHEN MATCHED AND ({differences}) THEN
                UPDATE SET {update_list}
            WHEN NOT MATCHED THEN
                INSERT ({target_list}) VALUES ({source_list})
            """
        )
        connection.execute(
            "DELETE FROM platform.feed_ingestion_state WHERE feed_name = ?",
            [feed_name],
        )
        connection.execute(
            """
            INSERT INTO platform.feed_ingestion_state
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            [
                feed_name,
                content_hash,
                source_rows,
                loaded_at,
                inserted_rows,
                updated_rows,
            ],
        )
        skipped_unchanged = False

    connection.execute(
        """
        INSERT INTO platform.feed_ingestion_events
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            run_id,
            feed_name,
            content_hash,
            source_rows,
            inserted_rows,
            updated_rows,
            skipped_unchanged,
            loaded_at,
        ],
    )
    return {
        "source_rows": source_rows,
        "inserted_rows": inserted_rows,
        "updated_rows": updated_rows,
        "skipped_unchanged": skipped_unchanged,
    }


def _f1(precision: float, recall: float) -> float:
    denominator = precision + recall
    return 2 * precision * recall / denominator if denominator else 0.0


def _read_cost_config(config_path: Path) -> dict[str, Any]:
    if not config_path.is_file():
        raise FileNotFoundError(f"Platform cost configuration not found: {config_path}")
    config = json.loads(config_path.read_text(encoding="utf-8"))
    numeric_fields = (
        "compute_usd_per_hour",
        "storage_usd_per_gib_month",
        "onboarding_hours_before_config",
        "onboarding_hours_after_config",
    )
    for field in numeric_fields:
        value = config.get(field)
        if not isinstance(value, (int, float)) or not math.isfinite(value) or value < 0:
            raise ValueError(f"Platform configuration field {field!r} must be a non-negative number")
    return config


def run_pipeline(
    raw_dir: Path,
    db_path: Path,
    schema_path: Path,
    transformations_path: Path | None = None,
    cost_config_path: Path | None = None,
    ontology_path: Path | None = None,
) -> dict[str, Any]:
    """Load raw CSVs, map product identities, and store benchmark metrics."""
    for filename in (
        *FEED_TABLES.keys(),
        "product_catalog.csv",
        "product_catalog_history.csv",
        "mapping_ground_truth.csv",
    ):
        path = raw_dir / filename
        if not path.is_file():
            raise FileNotFoundError(f"Required input file not found: {path}")
    if not schema_path.is_file():
        raise FileNotFoundError(f"DuckDB schema not found: {schema_path}")
    if transformations_path is None:
        transformations_path = schema_path.with_name("transformations.sql")
    if not transformations_path.is_file():
        raise FileNotFoundError(f"DuckDB transformations not found: {transformations_path}")
    if cost_config_path is None:
        cost_config_path = schema_path.parent.parent / "config" / "platform.json"
    cost_config = _read_cost_config(cost_config_path)
    if ontology_path is None:
        ontology_path = DEFAULT_ONTOLOGY_PATH
    ontology = load_ontology(ontology_path)

    db_path.parent.mkdir(parents=True, exist_ok=True)
    run_id = str(uuid.uuid4())
    started_at = datetime.now(timezone.utc)
    timer_started = time.perf_counter()
    input_bytes = sum((raw_dir / filename).stat().st_size for filename in FEED_TABLES)
    transform_fingerprint = hashlib.sha256(
        transformations_path.read_bytes()
        + (raw_dir / "product_catalog.csv").read_bytes()
        + (raw_dir / "product_catalog_history.csv").read_bytes()
    ).hexdigest()
    connection = duckdb.connect(str(db_path))
    try:
        connection.execute(schema_path.read_text(encoding="utf-8"))
        connection.execute("BEGIN TRANSACTION")
        try:
            previous_transform = connection.execute(
                """
                SELECT state_value
                FROM platform.transform_state
                WHERE state_key = 'silver_gold_transform'
                """
            ).fetchone()
            force_refresh = (
                previous_transform is None
                or previous_transform[0] != transform_fingerprint
            )
            connection.execute(
                "CREATE OR REPLACE TEMP TABLE _current_pipeline_run (run_id VARCHAR)"
            )
            connection.execute(
                "INSERT INTO _current_pipeline_run VALUES (?)",
                [run_id],
            )
            ingestion_events = {}
            for filename, table in FEED_TABLES.items():
                ingestion_events[filename] = _ingest_feed(
                    connection,
                    filename,
                    table,
                    raw_dir / filename,
                    run_id,
                    force_refresh,
                )
            _load_csv(connection, "reference_data.product_catalog", raw_dir / "product_catalog.csv")
            _load_csv(
                connection,
                "reference_data.product_catalog_history",
                raw_dir / "product_catalog_history.csv",
            )
            _load_csv(connection, "benchmark.mapping_ground_truth", raw_dir / "mapping_ground_truth.csv")

            catalog_rows = connection.execute(
                """
                SELECT product_id, canonical_sku, brand, category, product_name,
                       pack_size, pack_unit
                FROM reference_data.product_catalog
                ORDER BY product_id
                """
            ).fetchall()
            products = [
                dict(
                    zip(
                        (
                            "product_id",
                            "canonical_sku",
                            "brand",
                            "category",
                            "product_name",
                            "pack_size",
                            "pack_unit",
                        ),
                        row,
                    )
                )
                for row in catalog_rows
            ]
            versions = [
                release["version"] for release in ontology["mapping_releases"]
            ]
            release_aliases = {
                version: build_alias_index(products, ontology, version)
                for version in versions
            }
            production_version = versions[-1]
            production_aliases, production_fuzzy_aliases = release_aliases[
                production_version
            ]
            source_records: list[tuple[str, str, str, str | None, str | None, str, float, bool]] = []
            raw_products = connection.execute(
                """
                SELECT record_id, tenant_id, source_system, raw_product_value,
                       expected_product_id
                FROM benchmark.mapping_ground_truth
                ORDER BY record_id
                """
            ).fetchall()
            for record_id, tenant_id, source_system, raw_value, _ in raw_products:
                product_id, method, confidence = resolve_product(
                    raw_value,
                    production_aliases,
                    production_fuzzy_aliases,
                    ontology,
                    allow_fuzzy=True,
                )
                source_records.append(
                    (
                        record_id,
                        tenant_id,
                        source_system,
                        raw_value,
                        product_id,
                        method,
                        confidence,
                        product_id is None,
                    )
                )

            connection.execute(
                """
                CREATE OR REPLACE TEMP TABLE _mapping_stage (
                    record_id VARCHAR,
                    tenant_id VARCHAR,
                    source_system VARCHAR,
                    raw_product_value VARCHAR,
                    mapped_product_id VARCHAR,
                    mapping_method VARCHAR,
                    confidence DOUBLE,
                    review_required BOOLEAN
                )
                """
            )
            connection.executemany(
                "INSERT INTO _mapping_stage VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                source_records,
            )
            mapping_changes = connection.execute(
                f"""
                SELECT source.record_id,
                       CASE WHEN target.record_id IS NULL THEN 'INSERT'
                            WHEN {str(force_refresh).lower()} THEN 'REFRESH'
                            ELSE 'UPDATE' END
                FROM _mapping_stage AS source
                LEFT JOIN silver.product_mapping AS target USING (record_id)
                WHERE target.record_id IS NULL
                   OR target.tenant_id IS DISTINCT FROM source.tenant_id
                   OR target.source_system IS DISTINCT FROM source.source_system
                   OR target.raw_product_value IS DISTINCT FROM source.raw_product_value
                   OR target.mapped_product_id IS DISTINCT FROM source.mapped_product_id
                   OR target.mapping_method IS DISTINCT FROM source.mapping_method
                   OR target.confidence IS DISTINCT FROM source.confidence
                   OR target.review_required IS DISTINCT FROM source.review_required
                   OR {str(force_refresh).lower()}
                """
            ).fetchall()
            if mapping_changes:
                connection.executemany(
                    "INSERT INTO platform.feed_ingestion_changes VALUES (?, 'mapping', ?, ?)",
                    [
                        (run_id, record_id, change_type)
                        for record_id, change_type in mapping_changes
                    ],
                )
            connection.execute(
                """
                MERGE INTO silver.product_mapping AS target
                USING _mapping_stage AS source
                   ON target.record_id = source.record_id
                WHEN MATCHED AND (
                       target.tenant_id IS DISTINCT FROM source.tenant_id
                    OR target.source_system IS DISTINCT FROM source.source_system
                    OR target.raw_product_value IS DISTINCT FROM source.raw_product_value
                    OR target.mapped_product_id IS DISTINCT FROM source.mapped_product_id
                    OR target.mapping_method IS DISTINCT FROM source.mapping_method
                    OR target.confidence IS DISTINCT FROM source.confidence
                    OR target.review_required IS DISTINCT FROM source.review_required
                ) THEN UPDATE SET
                    tenant_id = source.tenant_id,
                    source_system = source.source_system,
                    raw_product_value = source.raw_product_value,
                    mapped_product_id = source.mapped_product_id,
                    mapping_method = source.mapping_method,
                    confidence = source.confidence,
                    review_required = source.review_required
                WHEN NOT MATCHED THEN
                    INSERT VALUES (
                        source.record_id, source.tenant_id, source.source_system,
                        source.raw_product_value, source.mapped_product_id,
                        source.mapping_method, source.confidence,
                        source.review_required
                    )
                """
            )

            metric_rows = connection.execute(
                """
                SELECT
                    labels.tenant_id,
                    labels.source_system,
                    count(*) FILTER (
                        WHERE mappings.mapped_product_id = labels.expected_product_id
                    ) AS true_positives,
                    count(*) FILTER (
                        WHERE mappings.mapped_product_id IS NOT NULL
                          AND mappings.mapped_product_id != labels.expected_product_id
                    ) AS false_positives,
                    count(*) FILTER (
                        WHERE mappings.mapped_product_id IS NULL
                           OR mappings.mapped_product_id != labels.expected_product_id
                    ) AS false_negatives,
                    count(*) FILTER (WHERE mappings.review_required) AS review_records
                FROM benchmark.mapping_ground_truth AS labels
                JOIN silver.product_mapping AS mappings USING (record_id)
                GROUP BY labels.tenant_id, labels.source_system
                ORDER BY labels.tenant_id, labels.source_system
                """
            ).fetchall()

            connection.execute("DELETE FROM benchmark.mapping_metrics")
            metrics: list[tuple[str, str, int, int, int, int, float, float, float]] = []
            for tenant_id, source_system, tp, fp, fn, reviews in metric_rows:
                precision = tp / (tp + fp) if tp + fp else 0.0
                recall = tp / (tp + fn) if tp + fn else 0.0
                metrics.append(
                    (
                        tenant_id,
                        source_system,
                        tp,
                        fp,
                        fn,
                        reviews,
                        precision,
                        recall,
                        _f1(precision, recall),
                    )
                )
            connection.executemany(
                "INSERT INTO benchmark.mapping_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                metrics,
            )

            release_metrics: list[tuple[str, str, str, int, int, int, float, float, float]] = []
            regression_results: list[tuple[str, str, int, int, str]] = []
            catalog_ids = {product["product_id"] for product in products}
            for version in versions:
                aliases, fuzzy_aliases = release_aliases[version]
                counts: dict[tuple[str, str], list[int]] = {}
                for (
                    _,
                    tenant_id,
                    source_system,
                    raw_value,
                    expected_id,
                ) in raw_products:
                    mapped_id, _, _ = resolve_product(
                        raw_value,
                        aliases,
                        fuzzy_aliases,
                        ontology,
                        allow_fuzzy=version == production_version,
                    )
                    key = (tenant_id, source_system)
                    tp, fp, fn = counts.setdefault(key, [0, 0, 0])
                    if mapped_id == expected_id:
                        counts[key][0] += 1
                    else:
                        if mapped_id is not None:
                            counts[key][1] += 1
                        counts[key][2] += 1
                for (tenant_id, source_system), (tp, fp, fn) in sorted(counts.items()):
                    precision = tp / (tp + fp) if tp + fp else 0.0
                    recall = tp / (tp + fn) if tp + fn else 0.0
                    release_metrics.append(
                        (
                            version,
                            tenant_id,
                            source_system,
                            tp,
                            fp,
                            fn,
                            precision,
                            recall,
                            _f1(precision, recall),
                        )
                    )

                canonical_failures = sum(
                    aliases.get(normalize_product_value(product["canonical_sku"]))
                    != (product["product_id"], "rule_exact")
                    for product in products
                )
                label_target_failures = connection.execute(
                    """
                    SELECT count(*)
                    FROM benchmark.mapping_ground_truth
                    WHERE expected_product_id NOT IN (
                        SELECT product_id FROM reference_data.product_catalog
                    )
                    """
                ).fetchone()[0]
                mapped_target_failures = sum(
                    product_id not in catalog_ids
                    for product_id, _ in aliases.values()
                )
                for check_name, checked_count, failed_count in (
                    ("canonical_product_keys", len(products), canonical_failures),
                    (
                        "ground_truth_targets",
                        len(raw_products),
                        label_target_failures,
                    ),
                    (
                        "mapped_targets_in_catalog",
                        len(aliases),
                        mapped_target_failures,
                    ),
                ):
                    regression_results.append(
                        (
                            version,
                            check_name,
                            checked_count,
                            failed_count,
                            "PASS" if failed_count == 0 else "FAIL",
                        )
                    )

            connection.execute("DELETE FROM benchmark.mapping_release_metrics")
            connection.executemany(
                "INSERT INTO benchmark.mapping_release_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                release_metrics,
            )
            connection.execute("DELETE FROM benchmark.ontology_regression_results")
            connection.executemany(
                "INSERT INTO benchmark.ontology_regression_results VALUES (?, ?, ?, ?, ?)",
                regression_results,
            )
            connection.execute(transformations_path.read_text(encoding="utf-8"))
            connection.execute(
                """
                DELETE FROM platform.transform_state
                WHERE state_key = 'silver_gold_transform'
                """
            )
            connection.execute(
                """
                INSERT INTO platform.transform_state
                VALUES ('silver_gold_transform', ?, current_timestamp)
                """,
                [transform_fingerprint],
            )

            feed_counts = {
                filename: connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for filename, table in FEED_TABLES.items()
            }
            tenant_source_counts = connection.execute(
                """
                SELECT tenant_id, source_system, count(*)
                FROM silver.product_mapping
                GROUP BY tenant_id, source_system
                ORDER BY tenant_id, source_system
                """
            ).fetchall()
            source_to_file = {
                "retailer_pos": "pos.csv",
                "warehouse": "shipments.csv",
                "trade_promotion": "promotions.csv",
                "syndicated": "syndicated.csv",
            }
            tenant_rows: dict[str, int] = {}
            tenant_bytes: dict[str, int] = {}
            for tenant_id, source_system, row_count in tenant_source_counts:
                filename = source_to_file[source_system]
                tenant_rows[tenant_id] = tenant_rows.get(tenant_id, 0) + row_count
                source_total = feed_counts[filename]
                allocated_bytes = round(
                    (raw_dir / filename).stat().st_size * row_count / source_total
                ) if source_total else 0
                tenant_bytes[tenant_id] = tenant_bytes.get(tenant_id, 0) + allocated_bytes

            input_records = sum(feed_counts.values())
            duration_seconds = time.perf_counter() - timer_started
            estimated_compute_usd = (
                duration_seconds * cost_config["compute_usd_per_hour"] / 3600
            )
            completed_at = datetime.now(timezone.utc)
            connection.execute(
                """
                INSERT INTO platform.pipeline_runs
                VALUES (?, ?, ?, ?, 'SUCCESS', ?, ?, 0, ?, NULL)
                """,
                [
                    run_id,
                    started_at,
                    completed_at,
                    duration_seconds,
                    input_records,
                    input_bytes,
                    estimated_compute_usd,
                ],
            )
            tenant_metrics = []
            for tenant_id, processed_rows in sorted(tenant_rows.items()):
                share = processed_rows / input_records if input_records else 0.0
                allocated_bytes = tenant_bytes[tenant_id]
                tenant_metrics.append(
                    (
                        run_id,
                        tenant_id,
                        processed_rows,
                        allocated_bytes,
                        duration_seconds * share,
                        estimated_compute_usd * share,
                        allocated_bytes
                        / (1024**3)
                        * cost_config["storage_usd_per_gib_month"],
                        cost_config["onboarding_hours_before_config"],
                        cost_config["onboarding_hours_after_config"],
                    )
                )
            connection.executemany(
                "INSERT INTO platform.tenant_run_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                tenant_metrics,
            )
            connection.execute("COMMIT")
        except Exception as error:
            connection.execute("ROLLBACK")
            completed_at = datetime.now(timezone.utc)
            duration_seconds = time.perf_counter() - timer_started
            connection.execute(
                """
                INSERT INTO platform.pipeline_runs
                VALUES (?, ?, ?, ?, 'FAILED', 0, ?, 0, ?, ?)
                """,
                [
                    run_id,
                    started_at,
                    completed_at,
                    duration_seconds,
                    input_bytes,
                    duration_seconds * cost_config["compute_usd_per_hour"] / 3600,
                    type(error).__name__,
                ],
            )
            raise

        summary = connection.execute(
            """
            SELECT
                count(*) AS labeled_records,
                count(*) FILTER (WHERE mapped_product_id IS NOT NULL) AS mapped_records,
                count(*) FILTER (WHERE review_required) AS review_records
            FROM silver.product_mapping
            """
        ).fetchone()
        gold_rows = connection.execute(
            "SELECT count(*) FROM gold.monthly_rgm"
        ).fetchone()[0]
        product_dimension_versions = connection.execute(
            "SELECT count(*) FROM silver.dim_product_scd2"
        ).fetchone()[0]
        contract_summary = connection.execute(
            """
            SELECT
                count(*) FILTER (WHERE status = 'PASS'),
                count(*) FILTER (WHERE status = 'WARN'),
                count(*) FILTER (WHERE status = 'FAIL')
            FROM benchmark.data_contract_results
            """
        ).fetchone()
        return {
            "database": str(db_path),
            "feed_row_counts": feed_counts,
            "labeled_records": summary[0],
            "mapped_records": summary[1],
            "review_records": summary[2],
            "metric_groups": len(metrics),
            "mapping_release_groups": len(release_metrics),
            "ontology_regression_checks": len(regression_results),
            "gold_monthly_rgm_rows": gold_rows,
            "product_dimension_versions": product_dimension_versions,
            "contract_checks_passed": contract_summary[0],
            "contract_checks_warned": contract_summary[1],
            "contract_checks_failed": contract_summary[2],
            "run_id": run_id,
            "duration_seconds": duration_seconds,
            "estimated_compute_usd": estimated_compute_usd,
            "input_bytes": input_bytes,
            "feed_ingestion": ingestion_events,
            "transform_full_refresh": force_refresh,
        }
    finally:
        connection.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-dir", type=Path, default=Path("data/raw"))
    parser.add_argument("--db-path", type=Path, default=Path("data/adp_demo.duckdb"))
    parser.add_argument("--schema", type=Path, default=Path("sql/schema.sql"))
    parser.add_argument(
        "--cost-config",
        type=Path,
        default=Path("config/platform.json"),
    )
    parser.add_argument("--ontology", type=Path, default=DEFAULT_ONTOLOGY_PATH)
    args = parser.parse_args()
    result = run_pipeline(
        args.raw_dir,
        args.db_path,
        args.schema,
        cost_config_path=args.cost_config,
        ontology_path=args.ontology,
    )
    print(json.dumps(result, indent=2, sort_keys=True))


if __name__ == "__main__":
    main()
