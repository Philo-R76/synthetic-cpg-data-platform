CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
CREATE SCHEMA IF NOT EXISTS reference_data;
CREATE SCHEMA IF NOT EXISTS benchmark;
CREATE SCHEMA IF NOT EXISTS platform;

CREATE TABLE IF NOT EXISTS bronze.pos_raw (
    record_id VARCHAR,
    tenant_id VARCHAR,
    source_system VARCHAR,
    transaction_date VARCHAR,
    store_id VARCHAR,
    retailer_sku VARCHAR,
    units_sold VARCHAR,
    net_sales VARCHAR,
    currency VARCHAR,
    sales_channel VARCHAR
);

CREATE TABLE IF NOT EXISTS bronze.shipments_raw (
    record_id VARCHAR,
    tenant_id VARCHAR,
    source_system VARCHAR,
    ship_date VARCHAR,
    warehouse_id VARCHAR,
    retailer_id VARCHAR,
    item_number VARCHAR,
    shipped_quantity VARCHAR,
    shipment_value VARCHAR,
    currency VARCHAR,
    unit_of_measure VARCHAR
);

CREATE TABLE IF NOT EXISTS bronze.promotions_raw (
    record_id VARCHAR,
    tenant_id VARCHAR,
    source_system VARCHAR,
    promotion_id VARCHAR,
    retailer_sku VARCHAR,
    start_date VARCHAR,
    end_date VARCHAR,
    discount_pct VARCHAR,
    promo_spend VARCHAR,
    currency VARCHAR
);

CREATE TABLE IF NOT EXISTS bronze.syndicated_raw (
    record_id VARCHAR,
    tenant_id VARCHAR,
    source_system VARCHAR,
    provider VARCHAR,
    market VARCHAR,
    period VARCHAR,
    product_code VARCHAR,
    market_units VARCHAR,
    market_sales VARCHAR,
    currency VARCHAR,
    unit_of_measure VARCHAR
);

CREATE TABLE IF NOT EXISTS reference_data.product_catalog (
    product_id VARCHAR PRIMARY KEY,
    canonical_sku VARCHAR NOT NULL,
    brand VARCHAR NOT NULL,
    category VARCHAR NOT NULL,
    product_name VARCHAR NOT NULL,
    pack_size VARCHAR,
    pack_unit VARCHAR
);

CREATE TABLE IF NOT EXISTS reference_data.product_catalog_history (
    product_id VARCHAR NOT NULL,
    canonical_sku VARCHAR NOT NULL,
    brand VARCHAR NOT NULL,
    category VARCHAR NOT NULL,
    product_name VARCHAR NOT NULL,
    pack_size VARCHAR,
    pack_unit VARCHAR,
    effective_from DATE NOT NULL,
    effective_to DATE,
    is_current BOOLEAN NOT NULL,
    PRIMARY KEY (product_id, effective_from)
);

CREATE TABLE IF NOT EXISTS benchmark.mapping_ground_truth (
    record_id VARCHAR PRIMARY KEY,
    tenant_id VARCHAR NOT NULL,
    source_system VARCHAR NOT NULL,
    raw_product_value VARCHAR,
    expected_product_id VARCHAR NOT NULL,
    expected_brand VARCHAR NOT NULL,
    expected_category VARCHAR NOT NULL,
    label_status VARCHAR NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.product_mapping (
    record_id VARCHAR PRIMARY KEY,
    tenant_id VARCHAR NOT NULL,
    source_system VARCHAR NOT NULL,
    raw_product_value VARCHAR,
    mapped_product_id VARCHAR,
    mapping_method VARCHAR NOT NULL,
    confidence DOUBLE NOT NULL,
    review_required BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS silver.dim_product_scd2 (
    product_version_key BIGINT PRIMARY KEY,
    product_id VARCHAR NOT NULL,
    canonical_sku VARCHAR NOT NULL,
    brand VARCHAR NOT NULL,
    category VARCHAR NOT NULL,
    product_name VARCHAR NOT NULL,
    pack_size VARCHAR,
    pack_unit VARCHAR,
    effective_from TIMESTAMP NOT NULL,
    effective_to TIMESTAMP,
    is_current BOOLEAN NOT NULL,
    attribute_hash VARCHAR NOT NULL,
    UNIQUE (product_id, effective_from)
);

CREATE TABLE IF NOT EXISTS benchmark.mapping_metrics (
    tenant_id VARCHAR NOT NULL,
    source_system VARCHAR NOT NULL,
    true_positives BIGINT NOT NULL,
    false_positives BIGINT NOT NULL,
    false_negatives BIGINT NOT NULL,
    review_records BIGINT NOT NULL,
    precision DOUBLE NOT NULL,
    recall DOUBLE NOT NULL,
    f1 DOUBLE NOT NULL,
    PRIMARY KEY (tenant_id, source_system)
);

CREATE TABLE IF NOT EXISTS benchmark.mapping_release_metrics (
    release_version VARCHAR NOT NULL,
    tenant_id VARCHAR NOT NULL,
    source_system VARCHAR NOT NULL,
    true_positives BIGINT NOT NULL,
    false_positives BIGINT NOT NULL,
    false_negatives BIGINT NOT NULL,
    precision DOUBLE NOT NULL,
    recall DOUBLE NOT NULL,
    f1 DOUBLE NOT NULL,
    PRIMARY KEY (release_version, tenant_id, source_system)
);

CREATE TABLE IF NOT EXISTS benchmark.ontology_regression_results (
    release_version VARCHAR NOT NULL,
    check_name VARCHAR NOT NULL,
    checked_count BIGINT NOT NULL,
    failed_count BIGINT NOT NULL,
    status VARCHAR NOT NULL,
    PRIMARY KEY (release_version, check_name)
);

CREATE TABLE IF NOT EXISTS benchmark.data_contract_results (
    check_name VARCHAR NOT NULL,
    tenant_id VARCHAR NOT NULL,
    row_count BIGINT NOT NULL,
    failed_count BIGINT NOT NULL,
    pass_rate DOUBLE NOT NULL,
    status VARCHAR NOT NULL,
    PRIMARY KEY (check_name, tenant_id)
);

CREATE TABLE IF NOT EXISTS platform.pipeline_runs (
    run_id VARCHAR PRIMARY KEY,
    started_at TIMESTAMP WITH TIME ZONE NOT NULL,
    completed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    duration_seconds DOUBLE NOT NULL,
    status VARCHAR NOT NULL,
    input_records BIGINT NOT NULL,
    input_bytes BIGINT NOT NULL,
    retry_count BIGINT NOT NULL,
    estimated_compute_usd DOUBLE NOT NULL,
    error_type VARCHAR
);

CREATE TABLE IF NOT EXISTS platform.tenant_run_metrics (
    run_id VARCHAR NOT NULL,
    tenant_id VARCHAR NOT NULL,
    processed_rows BIGINT NOT NULL,
    allocated_input_bytes BIGINT NOT NULL,
    allocated_runtime_seconds DOUBLE NOT NULL,
    estimated_compute_usd DOUBLE NOT NULL,
    estimated_storage_usd_per_month DOUBLE NOT NULL,
    onboarding_hours_before_config DOUBLE NOT NULL,
    onboarding_hours_after_config DOUBLE NOT NULL,
    PRIMARY KEY (run_id, tenant_id)
);

CREATE TABLE IF NOT EXISTS platform.feed_ingestion_state (
    feed_name VARCHAR PRIMARY KEY,
    content_sha256 VARCHAR NOT NULL,
    source_row_count BIGINT NOT NULL,
    last_loaded_at TIMESTAMP WITH TIME ZONE NOT NULL,
    inserted_rows BIGINT NOT NULL,
    updated_rows BIGINT NOT NULL
);

CREATE TABLE IF NOT EXISTS platform.feed_ingestion_events (
    run_id VARCHAR NOT NULL,
    feed_name VARCHAR NOT NULL,
    content_sha256 VARCHAR NOT NULL,
    source_rows BIGINT NOT NULL,
    inserted_rows BIGINT NOT NULL,
    updated_rows BIGINT NOT NULL,
    skipped_unchanged BOOLEAN NOT NULL,
    loaded_at TIMESTAMP WITH TIME ZONE NOT NULL,
    PRIMARY KEY (run_id, feed_name)
);

CREATE TABLE IF NOT EXISTS platform.feed_ingestion_changes (
    run_id VARCHAR NOT NULL,
    feed_name VARCHAR NOT NULL,
    record_id VARCHAR NOT NULL,
    change_type VARCHAR NOT NULL,
    PRIMARY KEY (run_id, feed_name, record_id)
);

CREATE TABLE IF NOT EXISTS platform.transform_state (
    state_key VARCHAR PRIMARY KEY,
    state_value VARCHAR NOT NULL,
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL
);
