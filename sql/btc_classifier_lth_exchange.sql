-- BTC on-chain classifier feed for Blockchain.py
-- Output schema:
-- timestamp, sopr, rhodl_ratio, exchange_net_flow,
-- lth_supply_change_pct, sth_supply_change_pct, miner_puell_multiple
--
-- This version avoids the expensive full input-output cost-basis join.
-- It adds:
--   - nonzero LTH/STH movement from bitcoin.inputs using spent age by block height
--   - best-effort BTC exchange net flow from cex.addresses joined to inputs/outputs
--   - live Puell-style miner behaviour from bitcoin.blocks
--
-- Notes:
--   - SOPR remains a fee-pressure proxy, not exact spent-output profit ratio.
--   - RHODL uses spent-age bands when available, otherwise falls back to block-demand proxy.
--   - If the CEX address join fails because of address type differences, remove the
--     exchange_* CTEs and set exchange_net_flow to 0 while keeping LTH/STH live.

WITH params AS (
    SELECT
        CURRENT_DATE - INTERVAL '395' day AS calc_start_date,
        CURRENT_DATE - INTERVAL '30' day AS output_start_date
),

daily_blocks AS (
    SELECT
        DATE_TRUNC('day', time) AS timestamp,
        COUNT(*) AS block_count,
        AVG(size) AS avg_block_size,
        SUM(total_fees) AS total_fees_btc,
        SUM(total_reward) AS miner_revenue_btc
    FROM bitcoin.blocks
    WHERE time >= (SELECT calc_start_date FROM params)
    GROUP BY 1
),

block_features AS (
    SELECT
        timestamp,
        block_count,
        avg_block_size,
        total_fees_btc,
        miner_revenue_btc,
        AVG(total_fees_btc) OVER (
            ORDER BY timestamp ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS fees_30d_avg,
        AVG(avg_block_size) OVER (
            ORDER BY timestamp ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        ) AS block_size_30d_avg,
        AVG(miner_revenue_btc) OVER (
            ORDER BY timestamp ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
        ) AS miner_revenue_365d_avg
    FROM daily_blocks
),

spent_age AS (
    SELECT
        DATE_TRUNC('day', block_time) AS timestamp,
        SUM(value) AS spent_btc,
        SUM(
            CASE
                WHEN (block_height - spent_block_height) >= 155 * 144 THEN value
                ELSE 0
            END
        ) AS lth_spent_btc,
        SUM(
            CASE
                WHEN (block_height - spent_block_height) < 155 * 144 THEN value
                ELSE 0
            END
        ) AS sth_spent_btc,
        SUM(
            CASE
                WHEN (block_height - spent_block_height) BETWEEN 1 * 144 AND 7 * 144 THEN value
                ELSE 0
            END
        ) AS one_week_spent_btc,
        SUM(
            CASE
                WHEN (block_height - spent_block_height) BETWEEN 365 * 144 AND 730 * 144 THEN value
                ELSE 0
            END
        ) AS one_to_two_year_spent_btc
    FROM bitcoin.inputs
    WHERE block_time >= (SELECT calc_start_date FROM params)
      AND is_coinbase = FALSE
      AND value > 0
      AND spent_block_height IS NOT NULL
    GROUP BY 1
),

holder_ratios AS (
    SELECT
        timestamp,
        lth_spent_btc / NULLIF(spent_btc, 0) AS lth_spent_share,
        sth_spent_btc / NULLIF(spent_btc, 0) AS sth_spent_share,
        one_week_spent_btc / NULLIF(one_to_two_year_spent_btc, 0) AS spent_age_rhodl_ratio
    FROM spent_age
),

holder_features AS (
    SELECT
        timestamp,
        lth_spent_share - LAG(lth_spent_share) OVER (ORDER BY timestamp) AS lth_supply_change_pct,
        sth_spent_share - LAG(sth_spent_share) OVER (ORDER BY timestamp) AS sth_supply_change_pct,
        spent_age_rhodl_ratio
    FROM holder_ratios
),

cex_addresses AS (
    SELECT DISTINCT CAST(address AS VARCHAR) AS address
    FROM cex.addresses
    WHERE blockchain = 'bitcoin'
),

exchange_inflow AS (
    SELECT
        DATE_TRUNC('day', o.block_time) AS timestamp,
        SUM(o.value) AS exchange_inflow_btc
    FROM bitcoin.outputs o
    JOIN cex_addresses c
      ON o.address = c.address
    WHERE o.block_time >= (SELECT calc_start_date FROM params)
    GROUP BY 1
),

exchange_outflow AS (
    SELECT
        DATE_TRUNC('day', i.block_time) AS timestamp,
        SUM(i.value) AS exchange_outflow_btc
    FROM bitcoin.inputs i
    JOIN cex_addresses c
      ON i.address = c.address
    WHERE i.block_time >= (SELECT calc_start_date FROM params)
      AND i.is_coinbase = FALSE
    GROUP BY 1
),

exchange_features AS (
    SELECT
        COALESCE(inflow.timestamp, outflow.timestamp) AS timestamp,
        COALESCE(inflow.exchange_inflow_btc, 0)
            - COALESCE(outflow.exchange_outflow_btc, 0) AS exchange_net_flow
    FROM exchange_inflow inflow
    FULL OUTER JOIN exchange_outflow outflow
      ON inflow.timestamp = outflow.timestamp
),

classifier_feed AS (
    SELECT
        b.timestamp,
        1.0 + ((b.total_fees_btc / NULLIF(b.fees_30d_avg, 0)) - 1.0) * 0.03 AS sopr,
        COALESCE(h.spent_age_rhodl_ratio * 100, 1000 + ((b.avg_block_size / NULLIF(b.block_size_30d_avg, 0)) - 1.0) * 900) AS rhodl_ratio,
        COALESCE(e.exchange_net_flow, 0) AS exchange_net_flow,
        COALESCE(h.lth_supply_change_pct, 0) AS lth_supply_change_pct,
        COALESCE(h.sth_supply_change_pct, 0) AS sth_supply_change_pct,
        b.miner_revenue_btc / NULLIF(b.miner_revenue_365d_avg, 0) AS miner_puell_multiple
    FROM block_features b
    LEFT JOIN holder_features h
      ON b.timestamp = h.timestamp
    LEFT JOIN exchange_features e
      ON b.timestamp = e.timestamp
)

SELECT
    timestamp,
    sopr,
    rhodl_ratio,
    exchange_net_flow,
    lth_supply_change_pct,
    sth_supply_change_pct,
    miner_puell_multiple
FROM classifier_feed
WHERE timestamp >= (SELECT output_start_date FROM params)
ORDER BY timestamp DESC;
