CREATE SEQUENCE IF NOT EXISTS silver.product_version_seq START 1;

UPDATE silver.dim_product_scd2 AS current_version
SET effective_to = coalesce(
        (
            SELECT min(history.effective_to)::TIMESTAMP
            FROM reference_data.product_catalog_history AS history
            WHERE history.product_id = current_version.product_id
              AND history.effective_to IS NOT NULL
              AND md5(
                  concat_ws(
                      '|',
                      history.canonical_sku,
                      history.brand,
                      history.category,
                      history.product_name,
                      coalesce(history.pack_size, ''),
                      coalesce(history.pack_unit, '')
                  )
              ) = current_version.attribute_hash
        ),
        current_timestamp
    ),
    is_current = false
FROM reference_data.product_catalog AS source
WHERE current_version.product_id = source.product_id
  AND current_version.is_current
  AND current_version.attribute_hash != md5(
      concat_ws(
          '|',
          source.canonical_sku,
          source.brand,
          source.category,
          source.product_name,
          coalesce(source.pack_size, ''),
          coalesce(source.pack_unit, '')
      )
  );

INSERT INTO silver.dim_product_scd2
SELECT
    nextval('silver.product_version_seq'),
    history.product_id,
    history.canonical_sku,
    history.brand,
    history.category,
    history.product_name,
    history.pack_size,
    history.pack_unit,
    history.effective_from::TIMESTAMP,
    history.effective_to::TIMESTAMP,
    history.is_current,
    md5(
        concat_ws(
            '|',
            history.canonical_sku,
            history.brand,
            history.category,
            history.product_name,
            coalesce(history.pack_size, ''),
            coalesce(history.pack_unit, '')
        )
    )
FROM reference_data.product_catalog_history AS history
WHERE NOT EXISTS (
    SELECT 1
    FROM silver.dim_product_scd2 AS existing
    WHERE existing.product_id = history.product_id
      AND existing.effective_from = history.effective_from::TIMESTAMP
);

INSERT INTO silver.dim_product_scd2
WITH source_rows AS (
    SELECT
        product_id,
        canonical_sku,
        brand,
        category,
        product_name,
        pack_size,
        pack_unit,
        md5(
            concat_ws(
                '|',
                canonical_sku,
                brand,
                category,
                product_name,
                coalesce(pack_size, ''),
                coalesce(pack_unit, '')
            )
        ) AS attribute_hash
    FROM reference_data.product_catalog
)
SELECT
    nextval('silver.product_version_seq'),
    source.product_id,
    source.canonical_sku,
    source.brand,
    source.category,
    source.product_name,
    source.pack_size,
    source.pack_unit,
    CASE
        WHEN EXISTS (
            SELECT 1
            FROM silver.dim_product_scd2 AS historical
            WHERE historical.product_id = source.product_id
        ) THEN current_timestamp
        ELSE TIMESTAMP '1900-01-01 00:00:00'
    END,
    NULL,
    true,
    source.attribute_hash
FROM source_rows AS source
LEFT JOIN silver.dim_product_scd2 AS previous
    ON source.product_id = previous.product_id
   AND previous.is_current
WHERE previous.product_id IS NULL
   OR previous.attribute_hash != source.attribute_hash;

CREATE OR REPLACE TEMP TABLE _changed_pos_ids AS
SELECT DISTINCT changes.record_id
FROM platform.feed_ingestion_changes AS changes
JOIN _current_pipeline_run AS run USING (run_id)
WHERE changes.feed_name IN ('pos.csv', 'mapping');

CREATE OR REPLACE TEMP TABLE _changed_shipment_ids AS
SELECT DISTINCT changes.record_id
FROM platform.feed_ingestion_changes AS changes
JOIN _current_pipeline_run AS run USING (run_id)
WHERE changes.feed_name IN ('shipments.csv', 'mapping');

CREATE OR REPLACE TEMP TABLE _changed_promotion_ids AS
SELECT DISTINCT changes.record_id
FROM platform.feed_ingestion_changes AS changes
JOIN _current_pipeline_run AS run USING (run_id)
WHERE changes.feed_name IN ('promotions.csv', 'mapping');

CREATE OR REPLACE TEMP TABLE _changed_syndicated_ids AS
SELECT DISTINCT changes.record_id
FROM platform.feed_ingestion_changes AS changes
JOIN _current_pipeline_run AS run USING (run_id)
WHERE changes.feed_name IN ('syndicated.csv', 'mapping');

CREATE OR REPLACE TEMP TABLE _pos_sales_stage AS
SELECT
    raw.record_id,
    raw.tenant_id,
    try_cast(raw.transaction_date AS DATE) AS sales_date,
    raw.store_id,
    mapping.mapped_product_id AS product_id,
    dimension.product_version_key,
    try_cast(raw.units_sold AS DOUBLE) AS units_sold,
    try_cast(raw.net_sales AS DECIMAL(18, 2)) AS net_sales,
    CASE raw.tenant_id
        WHEN 'Tenant_A' THEN 'USD'
        WHEN 'Tenant_B' THEN 'CAD'
        WHEN 'Tenant_C' THEN 'EUR'
        ELSE NULL
    END AS currency_code,
    raw.sales_channel
FROM bronze.pos_raw AS raw
JOIN silver.product_mapping AS mapping USING (record_id)
LEFT JOIN silver.dim_product_scd2 AS dimension
    ON mapping.mapped_product_id = dimension.product_id
   AND try_cast(raw.transaction_date AS TIMESTAMP) >= dimension.effective_from
   AND (
       dimension.effective_to IS NULL
       OR try_cast(raw.transaction_date AS TIMESTAMP) < dimension.effective_to
   )
WHERE mapping.source_system = 'retailer_pos'
  AND raw.record_id IN (SELECT record_id FROM _changed_pos_ids);

CREATE TABLE IF NOT EXISTS silver.pos_sales AS
SELECT * FROM _pos_sales_stage WHERE false;

CREATE OR REPLACE TEMP TABLE _affected_gold_keys AS
SELECT tenant_id, date_trunc('month', sales_date)::DATE AS sales_month,
       product_version_key, currency_code
FROM silver.pos_sales
WHERE false;

INSERT INTO _affected_gold_keys
SELECT tenant_id, date_trunc('month', sales_date)::DATE,
       product_version_key, currency_code
FROM silver.pos_sales
WHERE record_id IN (SELECT record_id FROM _changed_pos_ids)
  AND sales_date IS NOT NULL AND product_version_key IS NOT NULL
UNION
SELECT tenant_id, date_trunc('month', sales_date)::DATE,
       product_version_key, currency_code
FROM _pos_sales_stage
WHERE sales_date IS NOT NULL AND product_version_key IS NOT NULL;

CREATE OR REPLACE TEMP TABLE _shipments_stage AS
SELECT
    raw.record_id,
    raw.tenant_id,
    try_cast(raw.ship_date AS DATE) AS ship_date,
    raw.warehouse_id,
    raw.retailer_id,
    mapping.mapped_product_id AS product_id,
    dimension.product_version_key,
    try_cast(raw.shipped_quantity AS DOUBLE) AS shipped_quantity,
    try_cast(raw.shipment_value AS DECIMAL(18, 2)) AS shipment_value,
    CASE raw.tenant_id
        WHEN 'Tenant_A' THEN 'USD'
        WHEN 'Tenant_B' THEN 'CAD'
        WHEN 'Tenant_C' THEN 'EUR'
        ELSE NULL
    END AS currency_code,
    CASE
        WHEN upper(trim(raw.unit_of_measure)) IN ('EA', 'EACH', 'UNIT', 'UNITS') THEN 'EA'
        WHEN upper(trim(raw.unit_of_measure)) IN ('CASE', 'CS') THEN 'CASE'
        ELSE 'UNKNOWN'
    END AS unit_of_measure
FROM bronze.shipments_raw AS raw
JOIN silver.product_mapping AS mapping USING (record_id)
LEFT JOIN silver.dim_product_scd2 AS dimension
    ON mapping.mapped_product_id = dimension.product_id
   AND try_cast(raw.ship_date AS TIMESTAMP) >= dimension.effective_from
   AND (
       dimension.effective_to IS NULL
       OR try_cast(raw.ship_date AS TIMESTAMP) < dimension.effective_to
   )
WHERE mapping.source_system = 'warehouse'
  AND raw.record_id IN (SELECT record_id FROM _changed_shipment_ids);

CREATE TABLE IF NOT EXISTS silver.shipments AS
SELECT * FROM _shipments_stage WHERE false;

INSERT INTO _affected_gold_keys
SELECT tenant_id, date_trunc('month', ship_date)::DATE,
       product_version_key, currency_code
FROM silver.shipments
WHERE record_id IN (SELECT record_id FROM _changed_shipment_ids)
  AND ship_date IS NOT NULL AND product_version_key IS NOT NULL
UNION
SELECT tenant_id, date_trunc('month', ship_date)::DATE,
       product_version_key, currency_code
FROM _shipments_stage
WHERE ship_date IS NOT NULL AND product_version_key IS NOT NULL;

MERGE INTO silver.shipments AS target
USING _shipments_stage AS source
   ON target.record_id = source.record_id
WHEN MATCHED AND (
       target.tenant_id IS DISTINCT FROM source.tenant_id
    OR target.ship_date IS DISTINCT FROM source.ship_date
    OR target.warehouse_id IS DISTINCT FROM source.warehouse_id
    OR target.retailer_id IS DISTINCT FROM source.retailer_id
    OR target.product_id IS DISTINCT FROM source.product_id
    OR target.product_version_key IS DISTINCT FROM source.product_version_key
    OR target.shipped_quantity IS DISTINCT FROM source.shipped_quantity
    OR target.shipment_value IS DISTINCT FROM source.shipment_value
    OR target.currency_code IS DISTINCT FROM source.currency_code
    OR target.unit_of_measure IS DISTINCT FROM source.unit_of_measure
) THEN UPDATE SET
    tenant_id = source.tenant_id,
    ship_date = source.ship_date,
    warehouse_id = source.warehouse_id,
    retailer_id = source.retailer_id,
    product_id = source.product_id,
    product_version_key = source.product_version_key,
    shipped_quantity = source.shipped_quantity,
    shipment_value = source.shipment_value,
    currency_code = source.currency_code,
    unit_of_measure = source.unit_of_measure
WHEN NOT MATCHED THEN
    INSERT VALUES (
        source.record_id, source.tenant_id, source.ship_date, source.warehouse_id,
        source.retailer_id, source.product_id, source.product_version_key,
        source.shipped_quantity, source.shipment_value, source.currency_code,
        source.unit_of_measure
    );

CREATE OR REPLACE TEMP TABLE _promotions_stage AS
SELECT
    raw.record_id,
    raw.tenant_id,
    raw.promotion_id,
    mapping.mapped_product_id AS product_id,
    dimension.product_version_key,
    try_cast(raw.start_date AS DATE) AS start_date,
    try_cast(raw.end_date AS DATE) AS end_date,
    try_cast(raw.discount_pct AS DOUBLE) AS discount_pct,
    try_cast(raw.promo_spend AS DECIMAL(18, 2)) AS promo_spend,
    CASE raw.tenant_id
        WHEN 'Tenant_A' THEN 'USD'
        WHEN 'Tenant_B' THEN 'CAD'
        WHEN 'Tenant_C' THEN 'EUR'
        ELSE NULL
    END AS currency_code
FROM bronze.promotions_raw AS raw
JOIN silver.product_mapping AS mapping USING (record_id)
LEFT JOIN silver.dim_product_scd2 AS dimension
    ON mapping.mapped_product_id = dimension.product_id
   AND try_cast(raw.start_date AS TIMESTAMP) >= dimension.effective_from
   AND (
       dimension.effective_to IS NULL
       OR try_cast(raw.start_date AS TIMESTAMP) < dimension.effective_to
   )
WHERE mapping.source_system = 'trade_promotion'
  AND raw.record_id IN (SELECT record_id FROM _changed_promotion_ids);

CREATE TABLE IF NOT EXISTS silver.promotions AS
SELECT * FROM _promotions_stage WHERE false;

INSERT INTO _affected_gold_keys
SELECT tenant_id, date_trunc('month', start_date)::DATE,
       product_version_key, currency_code
FROM silver.promotions
WHERE record_id IN (SELECT record_id FROM _changed_promotion_ids)
  AND start_date IS NOT NULL AND product_version_key IS NOT NULL
UNION
SELECT tenant_id, date_trunc('month', start_date)::DATE,
       product_version_key, currency_code
FROM _promotions_stage
WHERE start_date IS NOT NULL AND product_version_key IS NOT NULL;

MERGE INTO silver.promotions AS target
USING _promotions_stage AS source
   ON target.record_id = source.record_id
WHEN MATCHED AND (
       target.tenant_id IS DISTINCT FROM source.tenant_id
    OR target.promotion_id IS DISTINCT FROM source.promotion_id
    OR target.product_id IS DISTINCT FROM source.product_id
    OR target.product_version_key IS DISTINCT FROM source.product_version_key
    OR target.start_date IS DISTINCT FROM source.start_date
    OR target.end_date IS DISTINCT FROM source.end_date
    OR target.discount_pct IS DISTINCT FROM source.discount_pct
    OR target.promo_spend IS DISTINCT FROM source.promo_spend
    OR target.currency_code IS DISTINCT FROM source.currency_code
) THEN UPDATE SET
    tenant_id = source.tenant_id,
    promotion_id = source.promotion_id,
    product_id = source.product_id,
    product_version_key = source.product_version_key,
    start_date = source.start_date,
    end_date = source.end_date,
    discount_pct = source.discount_pct,
    promo_spend = source.promo_spend,
    currency_code = source.currency_code
WHEN NOT MATCHED THEN
    INSERT VALUES (
        source.record_id, source.tenant_id, source.promotion_id,
        source.product_id, source.product_version_key, source.start_date,
        source.end_date, source.discount_pct, source.promo_spend,
        source.currency_code
    );

CREATE OR REPLACE TEMP TABLE _syndicated_stage AS
SELECT
    raw.record_id,
    raw.tenant_id,
    raw.provider,
    raw.market,
    try_cast(raw.period || '-01' AS DATE) AS period_start,
    mapping.mapped_product_id AS product_id,
    dimension.product_version_key,
    try_cast(raw.market_units AS DOUBLE) AS market_units,
    try_cast(raw.market_sales AS DECIMAL(18, 2)) AS market_sales,
    CASE raw.tenant_id
        WHEN 'Tenant_A' THEN 'USD'
        WHEN 'Tenant_B' THEN 'CAD'
        WHEN 'Tenant_C' THEN 'EUR'
        ELSE NULL
    END AS currency_code,
    CASE
        WHEN upper(trim(raw.unit_of_measure)) IN ('EA', 'UNIT', 'UNITS') THEN 'EA'
        WHEN upper(trim(raw.unit_of_measure)) IN ('CASE', 'CASES', 'CS') THEN 'CASE'
        ELSE 'UNKNOWN'
    END AS unit_of_measure
FROM bronze.syndicated_raw AS raw
JOIN silver.product_mapping AS mapping USING (record_id)
LEFT JOIN silver.dim_product_scd2 AS dimension
    ON mapping.mapped_product_id = dimension.product_id
   AND try_cast(raw.period || '-01' AS TIMESTAMP) >= dimension.effective_from
   AND (
       dimension.effective_to IS NULL
       OR try_cast(raw.period || '-01' AS TIMESTAMP) < dimension.effective_to
   )
WHERE mapping.source_system = 'syndicated'
  AND raw.record_id IN (SELECT record_id FROM _changed_syndicated_ids);

CREATE TABLE IF NOT EXISTS silver.syndicated AS
SELECT * FROM _syndicated_stage WHERE false;

INSERT INTO _affected_gold_keys
SELECT tenant_id, date_trunc('month', period_start)::DATE,
       product_version_key, currency_code
FROM silver.syndicated
WHERE record_id IN (SELECT record_id FROM _changed_syndicated_ids)
  AND period_start IS NOT NULL AND product_version_key IS NOT NULL
UNION
SELECT tenant_id, date_trunc('month', period_start)::DATE,
       product_version_key, currency_code
FROM _syndicated_stage
WHERE period_start IS NOT NULL AND product_version_key IS NOT NULL;

MERGE INTO silver.syndicated AS target
USING _syndicated_stage AS source
   ON target.record_id = source.record_id
WHEN MATCHED AND (
       target.tenant_id IS DISTINCT FROM source.tenant_id
    OR target.provider IS DISTINCT FROM source.provider
    OR target.market IS DISTINCT FROM source.market
    OR target.period_start IS DISTINCT FROM source.period_start
    OR target.product_id IS DISTINCT FROM source.product_id
    OR target.product_version_key IS DISTINCT FROM source.product_version_key
    OR target.market_units IS DISTINCT FROM source.market_units
    OR target.market_sales IS DISTINCT FROM source.market_sales
    OR target.currency_code IS DISTINCT FROM source.currency_code
    OR target.unit_of_measure IS DISTINCT FROM source.unit_of_measure
) THEN UPDATE SET
    tenant_id = source.tenant_id,
    provider = source.provider,
    market = source.market,
    period_start = source.period_start,
    product_id = source.product_id,
    product_version_key = source.product_version_key,
    market_units = source.market_units,
    market_sales = source.market_sales,
    currency_code = source.currency_code,
    unit_of_measure = source.unit_of_measure
WHEN NOT MATCHED THEN
    INSERT VALUES (
        source.record_id, source.tenant_id, source.provider, source.market,
        source.period_start, source.product_id, source.product_version_key,
        source.market_units, source.market_sales, source.currency_code,
        source.unit_of_measure
    );

MERGE INTO silver.pos_sales AS target
USING _pos_sales_stage AS source
   ON target.record_id = source.record_id
WHEN MATCHED AND (
       target.tenant_id IS DISTINCT FROM source.tenant_id
    OR target.sales_date IS DISTINCT FROM source.sales_date
    OR target.store_id IS DISTINCT FROM source.store_id
    OR target.product_id IS DISTINCT FROM source.product_id
    OR target.product_version_key IS DISTINCT FROM source.product_version_key
    OR target.units_sold IS DISTINCT FROM source.units_sold
    OR target.net_sales IS DISTINCT FROM source.net_sales
    OR target.currency_code IS DISTINCT FROM source.currency_code
    OR target.sales_channel IS DISTINCT FROM source.sales_channel
) THEN UPDATE SET
    tenant_id = source.tenant_id,
    sales_date = source.sales_date,
    store_id = source.store_id,
    product_id = source.product_id,
    product_version_key = source.product_version_key,
    units_sold = source.units_sold,
    net_sales = source.net_sales,
    currency_code = source.currency_code,
    sales_channel = source.sales_channel
WHEN NOT MATCHED THEN
    INSERT VALUES (
        source.record_id, source.tenant_id, source.sales_date, source.store_id,
        source.product_id, source.product_version_key, source.units_sold,
        source.net_sales, source.currency_code, source.sales_channel
    );

CREATE OR REPLACE TEMP TABLE _gold_monthly_rgm_stage AS
WITH pos AS (
    SELECT
        sales.tenant_id,
        date_trunc('month', sales.sales_date)::DATE AS sales_month,
        sales.product_id,
        sales.product_version_key,
        sales.currency_code,
        count(*) AS pos_transactions,
        sum(sales.units_sold) AS pos_units,
        sum(sales.net_sales) AS net_sales
    FROM silver.pos_sales AS sales
    JOIN silver.dim_product_scd2 AS catalog
      ON sales.product_version_key = catalog.product_version_key
    WHERE sales.sales_date IS NOT NULL
      AND sales.product_id IS NOT NULL
      AND sales.units_sold IS NOT NULL
      AND sales.net_sales IS NOT NULL
      AND EXISTS (
          SELECT 1
          FROM _affected_gold_keys AS affected
          WHERE affected.tenant_id = sales.tenant_id
            AND affected.sales_month = date_trunc('month', sales.sales_date)::DATE
            AND affected.product_version_key = sales.product_version_key
            AND affected.currency_code IS NOT DISTINCT FROM sales.currency_code
      )
    GROUP BY sales.tenant_id, sales_month, sales.product_id,
             sales.product_version_key, sales.currency_code
),
shipments AS (
    SELECT
        shipments.tenant_id,
        date_trunc('month', shipments.ship_date)::DATE AS sales_month,
        shipments.product_id,
        shipments.product_version_key,
        shipments.currency_code,
        sum(shipments.shipped_quantity) FILTER (WHERE shipments.unit_of_measure = 'EA') AS shipped_each_units,
        sum(shipments.shipped_quantity) FILTER (WHERE shipments.unit_of_measure = 'CASE') AS shipped_cases,
        sum(shipments.shipment_value) AS shipment_value
    FROM silver.shipments AS shipments
    JOIN silver.dim_product_scd2 AS dimension
      ON shipments.product_version_key = dimension.product_version_key
    WHERE shipments.ship_date IS NOT NULL
      AND shipments.product_id IS NOT NULL
      AND shipments.shipped_quantity IS NOT NULL
      AND EXISTS (
          SELECT 1
          FROM _affected_gold_keys AS affected
          WHERE affected.tenant_id = shipments.tenant_id
            AND affected.sales_month = date_trunc('month', shipments.ship_date)::DATE
            AND affected.product_version_key = shipments.product_version_key
            AND affected.currency_code IS NOT DISTINCT FROM shipments.currency_code
      )
    GROUP BY shipments.tenant_id, sales_month, shipments.product_id,
             shipments.product_version_key, shipments.currency_code
),
promotions AS (
    SELECT
        promotions.tenant_id,
        date_trunc('month', promotions.start_date)::DATE AS sales_month,
        promotions.product_id,
        promotions.product_version_key,
        promotions.currency_code,
        count(*) AS promotion_count,
        avg(promotions.discount_pct) AS avg_discount_pct,
        sum(promotions.promo_spend) AS promo_spend
    FROM silver.promotions AS promotions
    JOIN silver.dim_product_scd2 AS dimension
      ON promotions.product_version_key = dimension.product_version_key
    WHERE promotions.start_date IS NOT NULL
      AND promotions.product_id IS NOT NULL
      AND promotions.discount_pct IS NOT NULL
      AND EXISTS (
          SELECT 1
          FROM _affected_gold_keys AS affected
          WHERE affected.tenant_id = promotions.tenant_id
            AND affected.sales_month = date_trunc('month', promotions.start_date)::DATE
            AND affected.product_version_key = promotions.product_version_key
            AND affected.currency_code IS NOT DISTINCT FROM promotions.currency_code
      )
    GROUP BY promotions.tenant_id, sales_month, promotions.product_id,
             promotions.product_version_key, promotions.currency_code
),
keys AS (
    SELECT tenant_id, sales_month, product_id, product_version_key, currency_code
    FROM pos
    UNION
    SELECT tenant_id, sales_month, product_id, product_version_key, currency_code
    FROM shipments
    UNION
    SELECT tenant_id, sales_month, product_id, product_version_key, currency_code
    FROM promotions
)
SELECT
    keys.tenant_id,
    keys.sales_month,
    keys.product_id,
    keys.product_version_key,
    dimension.category,
    keys.currency_code,
    coalesce(pos.pos_transactions, 0) AS pos_transactions,
    coalesce(pos.pos_units, 0) AS pos_units,
    coalesce(pos.net_sales, 0) AS net_sales,
    coalesce(shipments.shipped_each_units, 0) AS shipped_each_units,
    coalesce(shipments.shipped_cases, 0) AS shipped_cases,
    coalesce(shipments.shipment_value, 0) AS shipment_value,
    coalesce(promotions.promotion_count, 0) AS promotion_count,
    promotions.avg_discount_pct,
    coalesce(promotions.promo_spend, 0) AS promo_spend,
    coalesce(shipments.shipped_each_units, 0) - coalesce(pos.pos_units, 0)
        AS shipment_pos_each_delta,
    coalesce(shipments.shipped_each_units, 0) / nullif(pos.pos_units, 0)
        AS shipment_to_pos_each_ratio
FROM keys
JOIN silver.dim_product_scd2 AS dimension
  ON keys.product_version_key = dimension.product_version_key
LEFT JOIN pos
  ON keys.tenant_id = pos.tenant_id
 AND keys.sales_month = pos.sales_month
 AND keys.product_id = pos.product_id
 AND keys.product_version_key = pos.product_version_key
 AND keys.currency_code IS NOT DISTINCT FROM pos.currency_code
LEFT JOIN shipments
  ON keys.tenant_id = shipments.tenant_id
 AND keys.sales_month = shipments.sales_month
 AND keys.product_id = shipments.product_id
 AND keys.product_version_key = shipments.product_version_key
 AND keys.currency_code IS NOT DISTINCT FROM shipments.currency_code
LEFT JOIN promotions
  ON keys.tenant_id = promotions.tenant_id
 AND keys.sales_month = promotions.sales_month
 AND keys.product_id = promotions.product_id
 AND keys.product_version_key = promotions.product_version_key
 AND keys.currency_code IS NOT DISTINCT FROM promotions.currency_code
;

CREATE TABLE IF NOT EXISTS gold.monthly_rgm AS
SELECT * FROM _gold_monthly_rgm_stage WHERE false;

CREATE UNIQUE INDEX IF NOT EXISTS gold_monthly_product_period_idx
    ON gold.monthly_rgm (
        tenant_id,
        sales_month,
        product_version_key,
        currency_code
    );

DELETE FROM gold.monthly_rgm AS target
WHERE EXISTS (
    SELECT 1
    FROM _affected_gold_keys AS affected
    WHERE affected.tenant_id = target.tenant_id
      AND affected.sales_month = target.sales_month
      AND affected.product_version_key = target.product_version_key
      AND affected.currency_code IS NOT DISTINCT FROM target.currency_code
)
AND NOT EXISTS (
    SELECT 1
    FROM _gold_monthly_rgm_stage AS source
    WHERE source.tenant_id = target.tenant_id
      AND source.sales_month = target.sales_month
      AND source.product_version_key = target.product_version_key
      AND source.currency_code IS NOT DISTINCT FROM target.currency_code
);

MERGE INTO gold.monthly_rgm AS target
USING _gold_monthly_rgm_stage AS source
   ON target.tenant_id = source.tenant_id
  AND target.sales_month = source.sales_month
  AND target.product_version_key = source.product_version_key
  AND target.currency_code IS NOT DISTINCT FROM source.currency_code
WHEN MATCHED THEN
    UPDATE SET
        product_id = source.product_id,
        category = source.category,
        pos_transactions = source.pos_transactions,
        pos_units = source.pos_units,
        net_sales = source.net_sales,
        shipped_each_units = source.shipped_each_units,
        shipped_cases = source.shipped_cases,
        shipment_value = source.shipment_value,
        promotion_count = source.promotion_count,
        avg_discount_pct = source.avg_discount_pct,
        promo_spend = source.promo_spend,
        shipment_pos_each_delta = source.shipment_pos_each_delta,
        shipment_to_pos_each_ratio = source.shipment_to_pos_each_ratio
WHEN NOT MATCHED THEN
    INSERT (
        tenant_id, sales_month, product_id, product_version_key, category,
        currency_code, pos_transactions, pos_units, net_sales,
        shipped_each_units, shipped_cases, shipment_value, promotion_count,
        avg_discount_pct, promo_spend, shipment_pos_each_delta,
        shipment_to_pos_each_ratio
    )
    VALUES (
        source.tenant_id, source.sales_month, source.product_id,
        source.product_version_key, source.category, source.currency_code,
        source.pos_transactions, source.pos_units, source.net_sales,
        source.shipped_each_units, source.shipped_cases, source.shipment_value,
        source.promotion_count, source.avg_discount_pct, source.promo_spend,
        source.shipment_pos_each_delta, source.shipment_to_pos_each_ratio
    );

DELETE FROM benchmark.data_contract_results;

INSERT INTO benchmark.data_contract_results
WITH checks AS (
    SELECT 'pos_date_parse' AS check_name, tenant_id, count(*) AS row_count,
           count(*) FILTER (WHERE sales_date IS NULL) AS failed_count,
           'FAIL' AS failure_status
    FROM silver.pos_sales GROUP BY tenant_id
    UNION ALL
    SELECT 'pos_units_parse', tenant_id, count(*),
           count(*) FILTER (WHERE units_sold IS NULL OR units_sold < 0), 'FAIL'
    FROM silver.pos_sales GROUP BY tenant_id
    UNION ALL
    SELECT 'pos_sales_parse', tenant_id, count(*),
           count(*) FILTER (WHERE net_sales IS NULL OR net_sales < 0), 'FAIL'
    FROM silver.pos_sales GROUP BY tenant_id
    UNION ALL
    SELECT 'shipment_date_parse', tenant_id, count(*),
           count(*) FILTER (WHERE ship_date IS NULL), 'FAIL'
    FROM silver.shipments GROUP BY tenant_id
    UNION ALL
    SELECT 'shipment_quantity_parse', tenant_id, count(*),
           count(*) FILTER (WHERE shipped_quantity IS NULL OR shipped_quantity < 0), 'FAIL'
    FROM silver.shipments GROUP BY tenant_id
    UNION ALL
    SELECT 'promotion_date_parse', tenant_id, count(*),
           count(*) FILTER (WHERE start_date IS NULL OR end_date IS NULL OR end_date < start_date), 'FAIL'
    FROM silver.promotions GROUP BY tenant_id
    UNION ALL
    SELECT 'syndicated_units_parse', tenant_id, count(*),
           count(*) FILTER (WHERE market_units IS NULL OR market_units < 0), 'FAIL'
    FROM silver.syndicated GROUP BY tenant_id
    UNION ALL
    SELECT 'product_mapping_review', tenant_id, count(*),
           count(*) FILTER (WHERE review_required), 'WARN'
    FROM silver.product_mapping GROUP BY tenant_id
)
SELECT
    check_name,
    tenant_id,
    row_count,
    failed_count,
    CASE WHEN row_count = 0 THEN 1.0
         ELSE (row_count - failed_count)::DOUBLE / row_count END AS pass_rate,
    CASE WHEN failed_count = 0 THEN 'PASS' ELSE failure_status END AS status
FROM checks;
