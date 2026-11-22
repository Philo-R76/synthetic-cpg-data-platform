"""Small read-only DuckDB query helpers shared by the Streamlit pages."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import duckdb
import pandas as pd
import streamlit as st


ROOT = Path(__file__).resolve().parents[1]
DEFAULT_DB_PATH = ROOT / "data" / "adp_demo.duckdb"


def database_ready(db_path: Path = DEFAULT_DB_PATH) -> bool:
    if not db_path.is_file():
        return False
    with duckdb.connect(str(db_path), read_only=True) as connection:
        tables = connection.execute(
            """
            SELECT table_schema, table_name
            FROM information_schema.tables
            WHERE table_schema IN ('bronze', 'silver', 'gold', 'benchmark', 'platform')
            """
        ).fetchall()
    available = {(schema, table) for schema, table in tables}
    return {
        ("bronze", "pos_raw"),
        ("silver", "product_mapping"),
        ("silver", "dim_product_scd2"),
        ("gold", "monthly_rgm"),
        ("benchmark", "mapping_metrics"),
        ("benchmark", "mapping_release_metrics"),
        ("benchmark", "ontology_regression_results"),
        ("benchmark", "data_contract_results"),
        ("platform", "pipeline_runs"),
        ("platform", "tenant_run_metrics"),
        ("platform", "feed_ingestion_events"),
    }.issubset(available)


@st.cache_data(ttl=30)
def query_frame(
    query: str,
    parameters: tuple[Any, ...] = (),
    db_path: str = str(DEFAULT_DB_PATH),
) -> pd.DataFrame:
    with duckdb.connect(db_path, read_only=True) as connection:
        return connection.execute(query, list(parameters)).fetchdf()


def get_overview(db_path: Path = DEFAULT_DB_PATH) -> dict[str, Any]:
    path = str(db_path)
    totals = query_frame(
        """
        SELECT
            count(*) AS labeled_records,
            count(*) FILTER (WHERE mapped_product_id IS NOT NULL) AS mapped_records,
            count(*) FILTER (WHERE review_required) AS review_records,
            count(DISTINCT tenant_id) AS tenants
        FROM silver.product_mapping
        """,
        db_path=path,
    ).iloc[0].to_dict()

    feeds = query_frame(
        """
        SELECT 'POS' AS feed, count(*) AS records FROM bronze.pos_raw
        UNION ALL
        SELECT 'Shipments', count(*) FROM bronze.shipments_raw
        UNION ALL
        SELECT 'Promotions', count(*) FROM bronze.promotions_raw
        UNION ALL
        SELECT 'Syndicated', count(*) FROM bronze.syndicated_raw
        ORDER BY feed
        """,
        db_path=path,
    )
    aggregate_metrics = query_frame(
        """
        SELECT
            sum(true_positives) AS true_positives,
            sum(false_positives) AS false_positives,
            sum(false_negatives) AS false_negatives,
            sum(true_positives) / nullif(
                sum(true_positives) + sum(false_positives), 0
            ) AS precision,
            sum(true_positives) / nullif(
                sum(true_positives) + sum(false_negatives), 0
            ) AS recall
        FROM benchmark.mapping_metrics
        """,
        db_path=path,
    ).iloc[0].to_dict()
    return {
        **totals,
        **aggregate_metrics,
        "feeds": feeds,
        "coverage": (
            totals["mapped_records"] / totals["labeled_records"]
            if totals["labeled_records"]
            else 0.0
        ),
    }


def get_tenants(db_path: Path = DEFAULT_DB_PATH) -> list[str]:
    frame = query_frame(
        "SELECT DISTINCT tenant_id FROM silver.product_mapping ORDER BY tenant_id",
        db_path=str(db_path),
    )
    return frame["tenant_id"].tolist()


def get_product_dimension_history(db_path: Path = DEFAULT_DB_PATH) -> pd.DataFrame:
    return query_frame(
        """
        SELECT product_version_key, product_id, canonical_sku, brand, category,
               product_name, effective_from, effective_to, is_current
        FROM silver.dim_product_scd2
        ORDER BY product_id, effective_from
        """,
        db_path=str(db_path),
    )


def get_mapping_metrics(
    tenant_id: str | None = None,
    db_path: Path = DEFAULT_DB_PATH,
) -> pd.DataFrame:
    query = """
        SELECT tenant_id, source_system, true_positives, false_positives,
               false_negatives, review_records, precision, recall, f1
        FROM benchmark.mapping_metrics
    """
    parameters: tuple[Any, ...] = ()
    if tenant_id is not None:
        query += " WHERE tenant_id = ?"
        parameters = (tenant_id,)
    query += " ORDER BY tenant_id, source_system"
    return query_frame(query, parameters, str(db_path))


def get_mapping_release_metrics(
    tenant_id: str | None = None,
    db_path: Path = DEFAULT_DB_PATH,
) -> pd.DataFrame:
    query = """
        SELECT release_version,
               sum(true_positives) AS true_positives,
               sum(false_positives) AS false_positives,
               sum(false_negatives) AS false_negatives,
               sum(true_positives) / nullif(
                   sum(true_positives) + sum(false_positives), 0
               ) AS precision,
               sum(true_positives) / nullif(
                   sum(true_positives) + sum(false_negatives), 0
               ) AS recall,
               2.0 * sum(true_positives) / nullif(
                   2 * sum(true_positives) + sum(false_positives)
                       + sum(false_negatives), 0
               ) AS f1
        FROM benchmark.mapping_release_metrics
    """
    parameters: tuple[Any, ...] = ()
    if tenant_id is not None:
        query += " WHERE tenant_id = ?"
        parameters = (tenant_id,)
    query += " GROUP BY release_version ORDER BY release_version"
    return query_frame(query, parameters, str(db_path))


def get_mapping_method_counts(
    tenant_id: str | None = None,
    db_path: Path = DEFAULT_DB_PATH,
) -> pd.DataFrame:
    query = """
        SELECT mapping_method, count(*) AS record_count,
               avg(confidence) AS average_confidence,
               count(*) FILTER (WHERE review_required) AS review_records
        FROM silver.product_mapping
    """
    parameters: tuple[Any, ...] = ()
    if tenant_id is not None:
        query += " WHERE tenant_id = ?"
        parameters = (tenant_id,)
    query += " GROUP BY mapping_method ORDER BY mapping_method"
    return query_frame(query, parameters, str(db_path))


def get_mapping_records(
    tenant_id: str | None = None,
    mapping_method: str | None = None,
    limit: int = 100,
    db_path: Path = DEFAULT_DB_PATH,
) -> pd.DataFrame:
    query = """
        SELECT mappings.record_id, mappings.tenant_id, mappings.source_system,
               mappings.raw_product_value, mappings.mapped_product_id,
               products.canonical_sku, products.brand, products.category,
               mappings.mapping_method, mappings.confidence,
               mappings.review_required
        FROM silver.product_mapping AS mappings
        LEFT JOIN reference_data.product_catalog AS products
          ON mappings.mapped_product_id = products.product_id
        WHERE true
    """
    parameters: list[Any] = []
    if tenant_id is not None:
        query += " AND mappings.tenant_id = ?"
        parameters.append(tenant_id)
    if mapping_method is not None:
        query += " AND mappings.mapping_method = ?"
        parameters.append(mapping_method)
    query += " ORDER BY mappings.review_required DESC, mappings.confidence, mappings.record_id LIMIT ?"
    parameters.append(limit)
    return query_frame(query, tuple(parameters), str(db_path))


def get_ontology_regression_results(
    db_path: Path = DEFAULT_DB_PATH,
) -> pd.DataFrame:
    return query_frame(
        """
        SELECT release_version, check_name, checked_count, failed_count, status
        FROM benchmark.ontology_regression_results
        ORDER BY release_version, check_name
        """,
        db_path=str(db_path),
    )


def get_product_catalog(db_path: Path = DEFAULT_DB_PATH) -> pd.DataFrame:
    return query_frame(
        """
        SELECT product_id, canonical_sku, brand, product_name, category,
               pack_size, pack_unit
        FROM reference_data.product_catalog
        ORDER BY category, brand, product_name
        """,
        db_path=str(db_path),
    )


def get_review_records(
    tenant_id: str | None = None,
    source_system: str | None = None,
    limit: int = 100,
    db_path: Path = DEFAULT_DB_PATH,
) -> pd.DataFrame:
    query = """
        SELECT
            mappings.record_id,
            mappings.tenant_id,
            mappings.source_system,
            mappings.raw_product_value,
            mappings.mapping_method,
            mappings.confidence,
            labels.expected_product_id,
            labels.expected_brand,
            labels.expected_category
        FROM silver.product_mapping AS mappings
        JOIN benchmark.mapping_ground_truth AS labels USING (record_id)
        WHERE mappings.review_required
    """
    parameters: list[Any] = []
    if tenant_id is not None:
        query += " AND mappings.tenant_id = ?"
        parameters.append(tenant_id)
    if source_system is not None:
        query += " AND mappings.source_system = ?"
        parameters.append(source_system)
    query += " ORDER BY mappings.tenant_id, mappings.source_system, mappings.record_id LIMIT ?"
    parameters.append(limit)
    return query_frame(query, tuple(parameters), str(db_path))


def get_categories(db_path: Path = DEFAULT_DB_PATH) -> list[str]:
    frame = query_frame(
        "SELECT DISTINCT category FROM reference_data.product_catalog ORDER BY category",
        db_path=str(db_path),
    )
    return frame["category"].tolist()


def get_gold_performance(
    tenant_id: str,
    category: str | None = None,
    db_path: Path = DEFAULT_DB_PATH,
) -> pd.DataFrame:
    query = """
        SELECT tenant_id, sales_month, product_id, category, currency_code,
               product_version_key, pos_transactions, pos_units, net_sales, shipped_each_units,
               shipped_cases, shipment_value, promotion_count,
               avg_discount_pct, promo_spend, shipment_pos_each_delta,
               shipment_to_pos_each_ratio
        FROM gold.monthly_rgm
        WHERE tenant_id = ?
    """
    parameters: tuple[Any, ...] = (tenant_id,)
    if category is not None:
        query += " AND category = ?"
        parameters += (category,)
    query += " ORDER BY sales_month, category, product_id"
    return query_frame(query, parameters, str(db_path))


def get_contract_results(
    tenant_id: str,
    db_path: Path = DEFAULT_DB_PATH,
) -> pd.DataFrame:
    return query_frame(
        """
        SELECT check_name, tenant_id, row_count, failed_count, pass_rate, status
        FROM benchmark.data_contract_results
        WHERE tenant_id = ?
        ORDER BY CASE status WHEN 'FAIL' THEN 0 WHEN 'WARN' THEN 1 ELSE 2 END,
                 check_name
        """,
        (tenant_id,),
        str(db_path),
    )


def get_pipeline_runs(
    limit: int = 50,
    db_path: Path = DEFAULT_DB_PATH,
) -> pd.DataFrame:
    return query_frame(
        """
        SELECT run_id, started_at, completed_at, duration_seconds, status,
               input_records, input_bytes, retry_count, estimated_compute_usd,
               error_type
        FROM platform.pipeline_runs
        ORDER BY started_at DESC
        LIMIT ?
        """,
        (limit,),
        str(db_path),
    )


def get_tenant_run_metrics(
    run_id: str,
    db_path: Path = DEFAULT_DB_PATH,
) -> pd.DataFrame:
    return query_frame(
        """
        SELECT tenant_id, processed_rows, allocated_input_bytes,
               allocated_runtime_seconds, estimated_compute_usd,
               estimated_storage_usd_per_month,
               onboarding_hours_before_config, onboarding_hours_after_config
        FROM platform.tenant_run_metrics
        WHERE run_id = ?
        ORDER BY tenant_id
        """,
        (run_id,),
        str(db_path),
    )


def get_feed_ingestion_events(
    run_id: str,
    db_path: Path = DEFAULT_DB_PATH,
) -> pd.DataFrame:
    return query_frame(
        """
        SELECT feed_name, source_rows, inserted_rows, updated_rows,
               skipped_unchanged, loaded_at
        FROM platform.feed_ingestion_events
        WHERE run_id = ?
        ORDER BY feed_name
        """,
        (run_id,),
        str(db_path),
    )
