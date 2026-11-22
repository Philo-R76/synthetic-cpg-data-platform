# ADP Synthetic CPG Data Demo

This starter generates reproducible, synthetic point-of-sale, shipment, promotion,
and syndicated-market feeds for three tenants. It also writes a product catalog
and row-level mapping labels so entity-resolution metrics can be measured against
known synthetic answers.

## Generate the data

From the project root:

```powershell
python -m src.generate_cpg_data
```

By default, the generator writes 12 months of data to `data/raw/` using seed 42.
Override the output directory, seed, period, or row volume as needed:

```powershell
python -m src.generate_cpg_data --output-dir data/raw --seed 42 --months 12 --records-per-month 24
```

The generator uses only the Python standard library. The output includes four
raw feeds, `product_catalog.csv`, `mapping_ground_truth.csv`,
`data_dictionary.json`, and `generation_manifest.json`. Missing values, invalid
dates, malformed numeric values, and currency/unit variations are intentional.
The benchmark labels are synthetic and must not be presented as real-world
accuracy results. Product aliases and synonym examples come from
`config/product_ontology.json`.

## DuckDB foundation

`sql/schema.sql` creates VARCHAR-first Bronze tables to preserve messy source
values, plus reference and benchmark tables. Use DuckDB to apply it; for example:

```powershell
python -c "import duckdb; c=duckdb.connect('data/adp_demo.duckdb'); c.execute(open('sql/schema.sql', encoding='utf-8').read()); c.close()"
```

## Run ingestion and mapping benchmark

After generating the raw feeds, run:

```powershell
python -m src.run_pipeline
```

The pipeline incrementally merges raw feed snapshots into Bronze and refreshes
the product catalog and ground-truth tables in `data/adp_demo.duckdb`. It applies
the configured matcher to build `silver.product_mapping`; unknown or missing
product values are assigned zero confidence and routed for human review.
Precision, recall, F1, and review counts are written to
`benchmark.mapping_metrics`, grouped by tenant and feed.
Release-by-release scores are written to `benchmark.mapping_release_metrics`.
The configured matcher progresses from canonical SKU through product names,
brand/pack aliases, ontology synonyms, and a cautious lexical-fuzzy fallback.
This fuzzy step is string similarity, not semantic embeddings or an LLM.
Regression checks for canonical keys and mapping targets are persisted in
`benchmark.ontology_regression_results`. These benchmark scores measure only
the included synthetic examples.

Bronze feed loads use file SHA-256 fingerprints to skip unchanged CSV parsing,
then `MERGE` changed snapshots by `record_id` so new rows are inserted and
changed rows updated. Rows absent from a later file are retained; deletion
handling would require explicit source tombstones. Per-run load counts are stored
in `platform.feed_ingestion_events`.

The pipeline also creates typed Silver tables for each source feed, a Gold
`gold.monthly_rgm` product-month data product, and tenant-level results in
`benchmark.data_contract_results`. Invalid dates or numbers remain visible as
failed contract checks instead of being silently coerced to valid data. Shipment
quantities are standardized to eaches or cases; only eaches are compared with
POS units. Promotion spend and discounts are descriptive, not a causal lift
estimate. `silver.dim_product_scd2` keeps product attribute history and the Silver fact
rows link to the dimension version valid on their business date. The seeded
SKU-1001 category transition creates a visible July 2025 history boundary;
unchanged runs do not add duplicate versions.

Changed Bronze records and changed mapping results are tracked by run ID.
Silver fact tables stage and merge only those records, while Gold recomputes
only the tenant/month/product-version/currency keys affected by the changed
facts. A transformation, current-catalog, or catalog-history fingerprint
forces a full refresh so logic or reference-data changes cannot leave stale
Silver/Gold values. Mapping benchmarks, data contracts, and operational
telemetry still scan or refresh broader tables. Rows removed from a source
snapshot are retained until explicit tombstone/delete propagation is added;
this is an incremental local demo, not a fully orchestrated production system.

## Launch the dashboard

Install the project's open-source Python dependencies, then run:

```powershell
python -m pip install -r requirements.txt
python -m streamlit run app.py
```

The app opens on a portfolio overview, with pages for release benchmarks,
ontology/synonym inspection, the unresolved synthetic-record review queue, Gold
RGM monthly sales and reconciliation, and platform governance. It reads the
local DuckDB database in read-only mode; regenerate and rerun the pipeline after
changing data or ontology configuration.

The Platform Governance page shows recorded local run duration, run status,
zero-retry behavior, and tenant allocations. Cost estimates and onboarding-hour
comparisons are controlled by `config/platform.json` and are explicitly
illustrative, not measured cloud spend or customer implementation outcomes.

## Tests

With DuckDB installed, generate sample data and run the test suite:

```powershell
python -m src.generate_cpg_data
python -m unittest discover -s tests -v
```
