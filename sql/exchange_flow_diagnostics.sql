-- Diagnose why direct BTC CEX exchange flow is returning zero.
--
-- Run each SELECT block separately in Dune if the full script UI complains
-- about multiple result sets.

-- 1) Confirm Dune has Bitcoin CEX labels and inspect address rendering.
SELECT
    blockchain,
    cex_name,
    typeof(address) AS address_type,
    CAST(address AS VARCHAR) AS cast_address_sample,
    COUNT(*) AS address_count
FROM cex.addresses
WHERE blockchain = 'bitcoin'
GROUP BY 1, 2, 3, 4
ORDER BY address_count DESC
LIMIT 50;

-- 2) Inspect recent Bitcoin output address formats.
SELECT
    type,
    address,
    COUNT(*) AS output_count,
    SUM(value) AS btc_value
FROM bitcoin.outputs
WHERE block_time >= NOW() - INTERVAL '7' day
  AND address IS NOT NULL
GROUP BY 1, 2
ORDER BY btc_value DESC
LIMIT 50;

-- 3) Test whether CAST(cex.addresses.address AS VARCHAR) matches outputs.
WITH cex_addresses AS (
    SELECT DISTINCT
        cex_name,
        CAST(address AS VARCHAR) AS address
    FROM cex.addresses
    WHERE blockchain = 'bitcoin'
),
matched_outputs AS (
    SELECT
        DATE_TRUNC('day', o.block_time) AS timestamp,
        c.cex_name,
        COUNT(*) AS output_count,
        SUM(o.value) AS inflow_btc
    FROM bitcoin.outputs o
    JOIN cex_addresses c
      ON o.address = c.address
    WHERE o.block_time >= NOW() - INTERVAL '30' day
    GROUP BY 1, 2
)
SELECT *
FROM matched_outputs
ORDER BY timestamp DESC, inflow_btc DESC
LIMIT 100;

-- 4) Test whether CAST(cex.addresses.address AS VARCHAR) matches inputs.
WITH cex_addresses AS (
    SELECT DISTINCT
        cex_name,
        CAST(address AS VARCHAR) AS address
    FROM cex.addresses
    WHERE blockchain = 'bitcoin'
),
matched_inputs AS (
    SELECT
        DATE_TRUNC('day', i.block_time) AS timestamp,
        c.cex_name,
        COUNT(*) AS input_count,
        SUM(i.value) AS outflow_btc
    FROM bitcoin.inputs i
    JOIN cex_addresses c
      ON i.address = c.address
    WHERE i.block_time >= NOW() - INTERVAL '30' day
      AND i.is_coinbase = FALSE
    GROUP BY 1, 2
)
SELECT *
FROM matched_inputs
ORDER BY timestamp DESC, outflow_btc DESC
LIMIT 100;
