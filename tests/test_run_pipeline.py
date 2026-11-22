import tempfile
import unittest
import csv
from pathlib import Path

import duckdb

from src.generate_cpg_data import generate_dataset
from src.run_pipeline import run_pipeline


class RunPipelineTests(unittest.TestCase):
    def test_pipeline_loads_feeds_maps_known_aliases_and_routes_missing_values(self):
        project_root = Path(__file__).parents[1]
        with tempfile.TemporaryDirectory() as temporary_dir:
            root = Path(temporary_dir)
            raw_dir = root / "raw"
            db_path = root / "demo.duckdb"
            generate_dataset(raw_dir, seed=3, months=1, records_per_month=3)

            summary = run_pipeline(raw_dir, db_path, project_root / "sql" / "schema.sql")
            self.assertEqual(summary["feed_row_counts"]["pos.csv"], 9)
            self.assertEqual(summary["labeled_records"], 36)
            self.assertEqual(summary["mapped_records"], 24)
            self.assertEqual(summary["review_records"], 12)
            self.assertEqual(summary["metric_groups"], 12)
            self.assertGreater(summary["gold_monthly_rgm_rows"], 0)
            self.assertEqual(summary["product_dimension_versions"], 9)
            self.assertEqual(summary["contract_checks_warned"], 3)
            self.assertGreater(summary["contract_checks_failed"], 0)
            self.assertGreater(summary["duration_seconds"], 0)
            self.assertGreater(summary["input_bytes"], 0)
            self.assertGreaterEqual(summary["estimated_compute_usd"], 0)
            self.assertEqual(summary["mapping_release_groups"], 48)
            self.assertEqual(summary["ontology_regression_checks"], 12)
            self.assertTrue(
                all(
                    event["inserted_rows"] == 9
                    and not event["skipped_unchanged"]
                    for event in summary["feed_ingestion"].values()
                )
            )

            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                self.assertEqual(
                    connection.execute(
                        """
                        SELECT sum(true_positives), sum(false_positives),
                               sum(false_negatives), sum(review_records)
                        FROM benchmark.mapping_metrics
                        """
                    ).fetchone(),
                    (24, 0, 12, 12),
                )
                self.assertEqual(
                    connection.execute(
                        """
                        SELECT count(*)
                        FROM silver.product_mapping
                        WHERE mapping_method = 'low_conf_hitl'
                          AND confidence = 0 AND review_required
                        """
                    ).fetchone()[0],
                    12,
                )
                self.assertEqual(
                    connection.execute(
                        """
                        SELECT count(*)
                        FROM silver.pos_sales
                        WHERE sales_date IS NULL OR units_sold IS NULL
                           OR product_id IS NULL
                        """
                    ).fetchone()[0],
                    9,
                )
                self.assertEqual(
                    connection.execute(
                        """
                        SELECT count(*)
                        FROM benchmark.data_contract_results
                        WHERE tenant_id = 'Tenant_A'
                          AND check_name = 'pos_date_parse'
                          AND failed_count = 1
                          AND status = 'FAIL'
                        """
                    ).fetchone()[0],
                    1,
                )
                self.assertGreater(
                    connection.execute(
                        "SELECT count(*) FROM gold.monthly_rgm WHERE promotion_count > 0"
                    ).fetchone()[0],
                    0,
                )
                self.assertEqual(
                    connection.execute(
                        """
                        SELECT status, retry_count, input_records, input_bytes
                        FROM platform.pipeline_runs
                        WHERE run_id = ?
                        """,
                        [summary["run_id"]],
                    ).fetchone(),
                    ("SUCCESS", 0, 36, summary["input_bytes"]),
                )
                self.assertEqual(
                    connection.execute(
                        """
                        SELECT count(*), sum(processed_rows),
                               min(onboarding_hours_before_config),
                               max(onboarding_hours_after_config)
                        FROM platform.tenant_run_metrics
                        WHERE run_id = ?
                        """,
                        [summary["run_id"]],
                    ).fetchone(),
                    (3, 36, 40.0, 4.0),
                )
                self.assertEqual(
                    connection.execute(
                        """
                        SELECT count(*), count(*) FILTER (WHERE is_current)
                        FROM silver.dim_product_scd2
                        """
                    ).fetchone(),
                    (9, 8),
                )
                self.assertEqual(
                    connection.execute(
                        """
                        SELECT count(*)
                        FROM silver.pos_sales
                        WHERE product_id IS NOT NULL
                          AND sales_date IS NOT NULL
                          AND product_version_key IS NULL
                        """
                    ).fetchone()[0],
                    0,
                )
                release_recall = connection.execute(
                    """
                    SELECT release_version, sum(true_positives),
                           sum(false_negatives)
                    FROM benchmark.mapping_release_metrics
                    GROUP BY release_version
                    ORDER BY release_version
                    """
                ).fetchall()
                self.assertEqual(len(release_recall), 4)
                self.assertEqual(release_recall[-1], ("v1.3", 24, 12))
                self.assertGreaterEqual(release_recall[-1][1], release_recall[0][1])
                self.assertEqual(
                    connection.execute(
                        """
                        SELECT count(*)
                        FROM benchmark.ontology_regression_results
                        WHERE status != 'PASS' OR failed_count != 0
                        """
                    ).fetchone()[0],
                    0,
                )
                self.assertEqual(
                    connection.execute(
                        """
                        SELECT count(*)
                        FROM benchmark.mapping_metrics
                        WHERE precision = 1 AND recall = 2.0 / 3.0
                        """
                    ).fetchone()[0],
                    12,
                )
            finally:
                connection.close()

            rerun = run_pipeline(
                raw_dir,
                db_path,
                project_root / "sql" / "schema.sql",
            )
            self.assertFalse(rerun["transform_full_refresh"])
            self.assertEqual(
                rerun["gold_monthly_rgm_rows"],
                summary["gold_monthly_rgm_rows"],
            )
            self.assertTrue(
                all(
                    event["skipped_unchanged"]
                    and event["inserted_rows"] == 0
                    and event["updated_rows"] == 0
                    for event in rerun["feed_ingestion"].values()
                )
            )
            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                self.assertEqual(
                    connection.execute(
                        """
                        SELECT count(*)
                        FROM platform.feed_ingestion_changes
                        WHERE run_id = ?
                        """,
                        [rerun["run_id"]],
                    ).fetchone()[0],
                    0,
                )
            finally:
                connection.close()

    def test_changed_feed_upserts_new_and_updated_rows(self):
        project_root = Path(__file__).parents[1]
        with tempfile.TemporaryDirectory() as temporary_dir:
            root = Path(temporary_dir)
            raw_dir = root / "raw"
            db_path = root / "demo.duckdb"
            generate_dataset(raw_dir, months=1, records_per_month=3)
            schema_path = project_root / "sql" / "schema.sql"
            run_pipeline(raw_dir, db_path, schema_path)
            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                baseline_gold_sales = connection.execute(
                    """
                    SELECT sum(net_sales)::DOUBLE
                    FROM gold.monthly_rgm
                    WHERE tenant_id = 'Tenant_A'
                      AND sales_month = DATE '2025-01-01'
                    """
                ).fetchone()[0]
            finally:
                connection.close()

            pos_path = raw_dir / "pos.csv"
            with pos_path.open(newline="", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                pos_fields = reader.fieldnames
                pos_rows = list(reader)
            pos_rows[0]["net_sales"] = "123.45"
            new_pos_row = dict(pos_rows[0])
            new_pos_row.update(
                {
                    "record_id": "pos-Tenant_A-202501-new",
                    "transaction_date": "2025-01-15",
                    "retailer_sku": "NS-OAT-1L",
                    "units_sold": "5",
                }
            )
            pos_rows.append(new_pos_row)
            with pos_path.open("w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=pos_fields)
                writer.writeheader()
                writer.writerows(pos_rows)

            labels_path = raw_dir / "mapping_ground_truth.csv"
            with labels_path.open(newline="", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                label_fields = reader.fieldnames
                labels = list(reader)
            labels.append(
                {
                    "record_id": "pos-Tenant_A-202501-new",
                    "tenant_id": "Tenant_A",
                    "source_system": "retailer_pos",
                    "raw_product_value": "NS-OAT-1L",
                    "expected_product_id": "SKU-1001",
                    "expected_brand": "Northstar",
                    "expected_category": "Plant-Based Beverages",
                    "label_status": "labeled",
                }
            )
            with labels_path.open("w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=label_fields)
                writer.writeheader()
                writer.writerows(labels)

            summary = run_pipeline(raw_dir, db_path, schema_path)
            self.assertFalse(summary["transform_full_refresh"])
            pos_event = summary["feed_ingestion"]["pos.csv"]
            self.assertEqual(pos_event["source_rows"], 10)
            self.assertEqual(pos_event["inserted_rows"], 1)
            self.assertEqual(pos_event["updated_rows"], 1)
            self.assertFalse(pos_event["skipped_unchanged"])
            self.assertTrue(
                all(
                    event["skipped_unchanged"]
                    for feed, event in summary["feed_ingestion"].items()
                    if feed != "pos.csv"
                )
            )

            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                self.assertEqual(
                    connection.execute("SELECT count(*) FROM bronze.pos_raw").fetchone()[0],
                    10,
                )
                updated_gold_sales = connection.execute(
                    """
                    SELECT sum(net_sales)::DOUBLE
                    FROM gold.monthly_rgm
                    WHERE tenant_id = 'Tenant_A'
                      AND sales_month = DATE '2025-01-01'
                    """
                ).fetchone()[0]
                self.assertAlmostEqual(
                    updated_gold_sales,
                    baseline_gold_sales + 123.45,
                )
                self.assertEqual(
                    connection.execute(
                        """
                        SELECT count(*)
                        FROM (
                            SELECT tenant_id, sales_month, product_version_key,
                                   currency_code
                            FROM gold.monthly_rgm
                            GROUP BY tenant_id, sales_month, product_version_key,
                                     currency_code
                            HAVING count(*) > 1
                        )
                        """
                    ).fetchone()[0],
                    0,
                )
                self.assertEqual(
                    connection.execute(
                        """
                        SELECT net_sales
                        FROM bronze.pos_raw
                        WHERE record_id = ?
                        """,
                        [pos_rows[0]["record_id"]],
                    ).fetchone()[0],
                    "123.45",
                )
            finally:
                connection.close()

    def test_schema_drift_is_reported_without_loading_partial_feed_data(self):
        project_root = Path(__file__).parents[1]
        with tempfile.TemporaryDirectory() as temporary_dir:
            root = Path(temporary_dir)
            raw_dir = root / "raw"
            db_path = root / "demo.duckdb"
            generate_dataset(raw_dir, months=1, records_per_month=3)

            pos_path = raw_dir / "pos.csv"
            with pos_path.open(newline="", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                fieldnames = [*reader.fieldnames, "unexpected_field"]
                rows = list(reader)
            for row in rows:
                row["unexpected_field"] = "schema-change"
            with pos_path.open("w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(rows)

            with self.assertRaisesRegex(ValueError, "Schema drift in pos.csv"):
                run_pipeline(
                    raw_dir,
                    db_path,
                    project_root / "sql" / "schema.sql",
                )

            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                self.assertEqual(
                    connection.execute("SELECT count(*) FROM bronze.pos_raw").fetchone()[0],
                    0,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT status, error_type FROM platform.pipeline_runs"
                    ).fetchone(),
                    ("FAILED", "ValueError"),
                )
            finally:
                connection.close()

    def test_pipeline_fails_for_missing_input_without_creating_partial_database(self):
        project_root = Path(__file__).parents[1]
        with tempfile.TemporaryDirectory() as temporary_dir:
            root = Path(temporary_dir)
            raw_dir = root / "empty"
            raw_dir.mkdir()
            db_path = root / "demo.duckdb"

            with self.assertRaises(FileNotFoundError):
                run_pipeline(raw_dir, db_path, project_root / "sql" / "schema.sql")
            self.assertFalse(db_path.exists())

    def test_failed_transformation_is_logged_and_rethrown(self):
        project_root = Path(__file__).parents[1]
        with tempfile.TemporaryDirectory() as temporary_dir:
            root = Path(temporary_dir)
            raw_dir = root / "raw"
            db_path = root / "demo.duckdb"
            bad_transform = root / "bad_transformations.sql"
            generate_dataset(raw_dir, months=1, records_per_month=3)
            bad_transform.write_text("THIS IS NOT VALID SQL;", encoding="utf-8")

            with self.assertRaises(duckdb.Error):
                run_pipeline(
                    raw_dir,
                    db_path,
                    project_root / "sql" / "schema.sql",
                    bad_transform,
                )

            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                self.assertEqual(
                    connection.execute(
                        "SELECT status, retry_count, error_type FROM platform.pipeline_runs"
                    ).fetchone(),
                    ("FAILED", 0, "ParserException"),
                )
                self.assertEqual(
                    connection.execute("SELECT count(*) FROM bronze.pos_raw").fetchone()[0],
                    0,
                )
            finally:
                connection.close()

    def test_product_attribute_change_creates_scd2_version_and_keeps_history(self):
        project_root = Path(__file__).parents[1]
        with tempfile.TemporaryDirectory() as temporary_dir:
            root = Path(temporary_dir)
            raw_dir = root / "raw"
            db_path = root / "demo.duckdb"
            generate_dataset(raw_dir, months=1, records_per_month=6)
            for filename, update_row in (
                (
                    "pos.csv",
                    lambda row: row["record_id"] == "pos-Tenant_A-202501-0003",
                ),
                (
                    "mapping_ground_truth.csv",
                    lambda row: row["record_id"] == "pos-Tenant_A-202501-0003",
                ),
            ):
                path = raw_dir / filename
                with path.open(newline="", encoding="utf-8") as file:
                    reader = csv.DictReader(file)
                    fieldnames = reader.fieldnames
                    rows = list(reader)
                for row in rows:
                    if update_row(row):
                        row["retailer_sku" if filename == "pos.csv" else "raw_product_value"] = "NS-OAT-1L"
                        if filename == "pos.csv":
                            row["transaction_date"] = "2025-01-03"
                            row["units_sold"] = "5"
                        else:
                            row["expected_product_id"] = "SKU-1001"
                            row["expected_brand"] = "Northstar"
                            row["expected_category"] = "Plant-Based Beverages"
                            row["label_status"] = "labeled"
                with path.open("w", newline="", encoding="utf-8") as file:
                    writer = csv.DictWriter(file, fieldnames=fieldnames)
                    writer.writeheader()
                    writer.writerows(rows)
            schema_path = project_root / "sql" / "schema.sql"
            run_pipeline(raw_dir, db_path, schema_path)

            catalog_path = raw_dir / "product_catalog.csv"
            with catalog_path.open(newline="", encoding="utf-8") as file:
                reader = csv.DictReader(file)
                fieldnames = reader.fieldnames
                products = list(reader)
            for product in products:
                if product["product_id"] == "SKU-1001":
                    product["category"] = "New Product Category"
            with catalog_path.open("w", newline="", encoding="utf-8") as file:
                writer = csv.DictWriter(file, fieldnames=fieldnames)
                writer.writeheader()
                writer.writerows(products)

            run_pipeline(raw_dir, db_path, schema_path)
            run_pipeline(raw_dir, db_path, schema_path)
            connection = duckdb.connect(str(db_path), read_only=True)
            try:
                versions = connection.execute(
                    """
                    SELECT category, effective_from, effective_to, is_current
                    FROM silver.dim_product_scd2
                    WHERE product_id = 'SKU-1001'
                    ORDER BY effective_from
                    """
                ).fetchall()
                self.assertEqual(len(versions), 3)
                self.assertEqual(versions[0][0], "Plant-Based Beverages")
                self.assertEqual(str(versions[0][2])[:10], "2025-07-01")
                self.assertIsNotNone(versions[0][2])
                self.assertFalse(versions[0][3])
                self.assertEqual(versions[1][0], "Plant Drinks")
                self.assertIsNotNone(versions[1][2])
                self.assertFalse(versions[1][3])
                self.assertEqual(versions[2][0], "New Product Category")
                self.assertTrue(versions[2][3])
                self.assertEqual(
                    connection.execute(
                        """
                        SELECT count(*)
                        FROM silver.dim_product_scd2
                        WHERE product_id = 'SKU-1001'
                        """
                    ).fetchone()[0],
                    3,
                )
                self.assertGreater(
                    connection.execute(
                        """
                        SELECT count(*)
                        FROM silver.product_mapping
                        WHERE mapping_method = 'fuzzy_lexical'
                        """
                    ).fetchone()[0],
                    0,
                )
                self.assertEqual(
                    connection.execute(
                        """
                        SELECT count(*)
                        FROM silver.pos_sales
                        WHERE product_id = 'SKU-1001'
                          AND product_version_key = (
                              SELECT product_version_key
                              FROM silver.dim_product_scd2
                              WHERE product_id = 'SKU-1001'
                                AND effective_from = TIMESTAMP '1900-01-01'
                          )
                        """
                    ).fetchone()[0],
                    1,
                )
            finally:
                connection.close()


if __name__ == "__main__":
    unittest.main()
