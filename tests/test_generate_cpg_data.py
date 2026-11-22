import csv
import tempfile
import unittest
from pathlib import Path

import duckdb

from src.generate_cpg_data import FEED_FIELDS, generate_dataset


class GenerateCpgDataTests(unittest.TestCase):
    def test_generation_is_deterministic_and_labels_every_feed_row(self):
        with tempfile.TemporaryDirectory() as first_dir, tempfile.TemporaryDirectory() as second_dir:
            first = Path(first_dir)
            second = Path(second_dir)
            manifest = generate_dataset(first, seed=7, months=1, records_per_month=4)
            generate_dataset(second, seed=7, months=1, records_per_month=4)

            generated_files = sorted(path.name for path in first.iterdir())
            self.assertEqual(generated_files, sorted(path.name for path in second.iterdir()))
            for filename in generated_files:
                self.assertEqual((first / filename).read_bytes(), (second / filename).read_bytes())

            self.assertEqual(manifest["ground_truth_label_count"], 4 * 3 * 4)
            with (first / "mapping_ground_truth.csv").open(
                newline="", encoding="utf-8"
            ) as file:
                labels = list(csv.DictReader(file))
            self.assertEqual(len(labels), manifest["ground_truth_label_count"])
            self.assertTrue(any(label["label_status"] == "missing_raw_value" for label in labels))

    def test_manifest_counts_anomalous_feed_rows(self):
        with tempfile.TemporaryDirectory() as output_dir:
            manifest = generate_dataset(
                Path(output_dir), months=1, records_per_month=3
            )

        self.assertEqual(
            manifest["injected_anomalies"],
            {
                "invalid_dates": 9,
                "missing_product_values": 12,
                "invalid_numeric_values": 9,
            },
        )

    def test_duckdb_schema_accepts_generated_csvs_without_type_inference(self):
        with tempfile.TemporaryDirectory() as output_dir:
            data_dir = Path(output_dir)
            generate_dataset(data_dir, months=1, records_per_month=3)
            connection = duckdb.connect(":memory:")
            try:
                schema_path = Path(__file__).parents[1] / "sql" / "schema.sql"
                connection.execute(schema_path.read_text(encoding="utf-8"))
                table_targets = {
                    "pos.csv": "bronze.pos_raw",
                    "shipments.csv": "bronze.shipments_raw",
                    "promotions.csv": "bronze.promotions_raw",
                    "syndicated.csv": "bronze.syndicated_raw",
                }
                for filename, table in table_targets.items():
                    connection.execute(
                        f"INSERT INTO {table} SELECT * FROM read_csv(?, header=true, all_varchar=true)",
                        [str(data_dir / filename)],
                    )
                    self.assertEqual(
                        connection.execute(f"SELECT count(*) FROM {table}").fetchone()[0],
                        9,
                    )
                    columns = [
                        row[0]
                        for row in connection.execute(f"DESCRIBE {table}").fetchall()
                    ]
                    self.assertEqual(columns, list(FEED_FIELDS[filename]))

                for filename, table in (
                    ("product_catalog.csv", "reference_data.product_catalog"),
                    ("mapping_ground_truth.csv", "benchmark.mapping_ground_truth"),
                ):
                    connection.execute(
                        f"INSERT INTO {table} SELECT * FROM read_csv(?, header=true, all_varchar=true)",
                        [str(data_dir / filename)],
                    )
                self.assertEqual(
                    connection.execute(
                        "SELECT count(*) FROM reference_data.product_catalog"
                    ).fetchone()[0],
                    8,
                )
                self.assertEqual(
                    connection.execute(
                        "SELECT count(*) FROM benchmark.mapping_ground_truth"
                    ).fetchone()[0],
                    36,
                )
            finally:
                connection.close()


if __name__ == "__main__":
    unittest.main()
