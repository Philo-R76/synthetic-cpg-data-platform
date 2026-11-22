import tempfile
import unittest
from pathlib import Path

from src.dashboard_data import (
    database_ready,
    get_contract_results,
    get_feed_ingestion_events,
    get_gold_performance,
    get_mapping_metrics,
    get_mapping_method_counts,
    get_mapping_records,
    get_mapping_release_metrics,
    get_ontology_regression_results,
    get_overview,
    get_pipeline_runs,
    get_product_dimension_history,
    get_tenants,
    get_review_records,
    get_tenant_run_metrics,
    query_frame,
)
from src.generate_cpg_data import generate_dataset
from src.run_pipeline import run_pipeline


class DashboardDataTests(unittest.TestCase):
    def test_dashboard_queries_return_filtered_benchmark_and_review_data(self):
        project_root = Path(__file__).parents[1]
        with tempfile.TemporaryDirectory() as temporary_dir:
            root = Path(temporary_dir)
            raw_dir = root / "raw"
            db_path = root / "demo.duckdb"
            generate_dataset(raw_dir, months=1, records_per_month=3)
            run_pipeline(raw_dir, db_path, project_root / "sql" / "schema.sql")

            query_frame.clear()
            self.assertTrue(database_ready(db_path))
            self.assertEqual(get_tenants(db_path), ["Tenant_A", "Tenant_B", "Tenant_C"])
            overview = get_overview(db_path)
            self.assertEqual(overview["labeled_records"], 36)
            self.assertEqual(overview["mapped_records"], 24)
            self.assertEqual(overview["review_records"], 12)
            self.assertEqual(overview["coverage"], 2 / 3)
            self.assertEqual(len(overview["feeds"]), 4)

            metrics = get_mapping_metrics("Tenant_A", db_path)
            self.assertEqual(len(metrics), 4)
            self.assertEqual(set(metrics["tenant_id"]), {"Tenant_A"})
            release_metrics = get_mapping_release_metrics("Tenant_A", db_path)
            self.assertEqual(list(release_metrics["release_version"]), ["v1.0", "v1.1", "v1.2", "v1.3"])
            method_counts = get_mapping_method_counts("Tenant_A", db_path)
            self.assertIn("ontology_synonym", set(method_counts["mapping_method"]))
            record_samples = get_mapping_records(
                "Tenant_A", "ontology_synonym", 10, db_path
            )
            self.assertGreater(len(record_samples), 0)
            regression = get_ontology_regression_results(db_path)
            self.assertEqual(len(regression), 12)
            self.assertEqual(set(regression["status"]), {"PASS"})

            gold = get_gold_performance("Tenant_A", db_path=db_path)
            self.assertFalse(gold.empty)
            self.assertEqual(set(gold["currency_code"].dropna()), {"USD"})
            self.assertIn("product_version_key", gold.columns)
            self.assertEqual(len(get_product_dimension_history(db_path)), 9)
            contracts = get_contract_results("Tenant_A", db_path)
            self.assertEqual(len(contracts), 8)
            self.assertIn("WARN", set(contracts["status"]))

            runs = get_pipeline_runs(db_path=db_path)
            self.assertEqual(len(runs), 1)
            self.assertEqual(runs.iloc[0]["status"], "SUCCESS")
            feed_events = get_feed_ingestion_events(runs.iloc[0]["run_id"], db_path)
            self.assertEqual(len(feed_events), 4)
            self.assertFalse(feed_events["skipped_unchanged"].any())
            tenant_metrics = get_tenant_run_metrics(runs.iloc[0]["run_id"], db_path)
            self.assertEqual(len(tenant_metrics), 3)

            queue = get_review_records("Tenant_A", "retailer_pos", 10, db_path)
            self.assertEqual(len(queue), 1)
            self.assertEqual(queue.iloc[0]["mapping_method"], "low_conf_hitl")
            query_frame.clear()


if __name__ == "__main__":
    unittest.main()
