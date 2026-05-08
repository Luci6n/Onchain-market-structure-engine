-- Probe Dune's cex.flows schema and actual flow_type values.
-- Run this as a temporary diagnostic query.

-- 1) Column names and data types.
SELECT
    column_name,
    data_type
FROM information_schema.columns
WHERE table_schema = 'cex'
  AND table_name = 'flows'
ORDER BY ordinal_position;

-- 2) Actual Ethereum flow_type values and whether amount_usd is populated.
-- If Dune does not allow multiple result sets, run only this SELECT.
SELECT
    flow_type,
    COUNT(*) AS flow_rows,
    COUNT(amount_usd) AS amount_usd_rows,
    SUM(amount_usd) AS total_amount_usd,
    MIN(amount_usd) AS min_amount_usd,
    MAX(amount_usd) AS max_amount_usd
FROM cex.flows
WHERE blockchain = 'ethereum'
  AND block_time >= NOW() - INTERVAL '30' day
GROUP BY 1
ORDER BY flow_rows DESC;

-- 3) A few recent rows to inspect naming.
SELECT
    block_time,
    blockchain,
    flow_type,
    amount_usd,
    token_symbol,
    cex_name
FROM cex.flows
WHERE blockchain = 'ethereum'
  AND block_time >= NOW() - INTERVAL '7' day
ORDER BY block_time DESC
LIMIT 50;
