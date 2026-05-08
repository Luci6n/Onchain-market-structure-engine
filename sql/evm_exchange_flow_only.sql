-- Quick Dune diagnostic for Ethereum CEX flow.
-- This groups by actual flow_type values. For cex.flows, the main values are
-- usually Inflow and Outflow.

SELECT
    DATE_TRUNC('day', block_time) AS timestamp,
    flow_type,
    COUNT(*) AS flow_rows,
    COUNT(amount_usd) AS amount_usd_rows,
    SUM(amount_usd) AS total_amount_usd
FROM cex.flows
WHERE blockchain = 'ethereum'
  AND block_time >= NOW() - INTERVAL '30' day
  AND amount_usd IS NOT NULL
GROUP BY 1, 2
ORDER BY timestamp DESC, flow_rows DESC;
