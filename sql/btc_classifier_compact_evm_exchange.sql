-- Compact BTC classifier feed with EVM CEX exchange flow.
-- Output schema expected by Blockchain.py:
-- timestamp, sopr, rhodl_ratio, exchange_net_flow,
-- lth_supply_change_pct, sth_supply_change_pct, miner_puell_multiple

WITH params AS (
    SELECT
        CURRENT_DATE - INTERVAL '395' day AS calc_start_date,
        CURRENT_DATE - INTERVAL '30' day AS output_start_date
),

btc_price AS (
    SELECT
        DATE_TRUNC('day', timestamp) AS timestamp,
        AVG(price) AS btc_price_usd
    FROM prices.day
    WHERE symbol = 'BTC'
      AND timestamp >= (SELECT calc_start_date FROM params)
    GROUP BY 1
),

block_features AS (
    SELECT
        timestamp,
        1.0 + ((total_fees_btc / NULLIF(fees_30d_avg, 0)) - 1.0) * 0.03 AS sopr,
        1000 + ((avg_block_size / NULLIF(block_size_30d_avg, 0)) - 1.0) * 900 AS rhodl_ratio,
        miner_revenue_btc / NULLIF(miner_revenue_365d_avg, 0) AS miner_puell_multiple
    FROM (
        SELECT
            DATE_TRUNC('day', time) AS timestamp,
            AVG(size) AS avg_block_size,
            SUM(total_fees) AS total_fees_btc,
            SUM(total_reward) AS miner_revenue_btc,
            AVG(SUM(total_fees)) OVER (
                ORDER BY DATE_TRUNC('day', time)
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS fees_30d_avg,
            AVG(AVG(size)) OVER (
                ORDER BY DATE_TRUNC('day', time)
                ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
            ) AS block_size_30d_avg,
            AVG(SUM(total_reward)) OVER (
                ORDER BY DATE_TRUNC('day', time)
                ROWS BETWEEN 364 PRECEDING AND CURRENT ROW
            ) AS miner_revenue_365d_avg
        FROM bitcoin.blocks
        WHERE time >= (SELECT calc_start_date FROM params)
        GROUP BY 1
    )
),

holder_daily AS (
    SELECT
        timestamp,
        lth_share - LAG(lth_share) OVER (ORDER BY timestamp) AS lth_supply_change_pct,
        sth_share - LAG(sth_share) OVER (ORDER BY timestamp) AS sth_supply_change_pct
    FROM (
        SELECT
            DATE_TRUNC('day', block_time) AS timestamp,
            SUM(CASE WHEN (block_height - spent_block_height) >= 22320 THEN value ELSE 0 END) / NULLIF(SUM(value), 0) AS lth_share,
            SUM(CASE WHEN (block_height - spent_block_height) < 22320 THEN value ELSE 0 END) / NULLIF(SUM(value), 0) AS sth_share
        FROM bitcoin.inputs
        WHERE block_time >= (SELECT calc_start_date FROM params)
          AND is_coinbase = FALSE
          AND value > 0
          AND spent_block_height IS NOT NULL
        GROUP BY 1
    )
),

exchange_daily AS (
    SELECT
        flow.timestamp,
        flow.exchange_net_flow_usd / NULLIF(price.btc_price_usd, 0) AS exchange_net_flow
    FROM (
        SELECT
            DATE_TRUNC('day', block_time) AS timestamp,
            SUM(
                CASE
                    WHEN flow_type = 'Inflow' THEN amount_usd
                    WHEN flow_type = 'Outflow' THEN 0 - amount_usd
                    ELSE 0
                END
            ) AS exchange_net_flow_usd
        FROM cex.flows
        WHERE blockchain = 'ethereum'
          AND block_time >= (SELECT calc_start_date FROM params)
          AND amount_usd IS NOT NULL
        GROUP BY 1
    ) flow
    LEFT JOIN btc_price price
      ON flow.timestamp = price.timestamp
)

SELECT
    block_features.timestamp,
    block_features.sopr,
    block_features.rhodl_ratio,
    COALESCE(exchange_daily.exchange_net_flow, 0) AS exchange_net_flow,
    COALESCE(holder_daily.lth_supply_change_pct, 0) AS lth_supply_change_pct,
    COALESCE(holder_daily.sth_supply_change_pct, 0) AS sth_supply_change_pct,
    block_features.miner_puell_multiple
FROM block_features
LEFT JOIN holder_daily
  ON block_features.timestamp = holder_daily.timestamp
LEFT JOIN exchange_daily
  ON block_features.timestamp = exchange_daily.timestamp
WHERE block_features.timestamp >= (SELECT output_start_date FROM params)
ORDER BY block_features.timestamp DESC;
