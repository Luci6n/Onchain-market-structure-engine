-- BTC on-chain classifier feed with EVM CEX exchange-flow signal.
-- Output schema:
-- timestamp, sopr, rhodl_ratio, exchange_net_flow,
-- lth_supply_change_pct, sth_supply_change_pct, miner_puell_multiple
--
-- Why this exists:
-- Dune's cex.addresses table includes Bitcoin labels, but direct joins to
-- bitcoin.inputs / bitcoin.outputs can be sparse or address-format sensitive.
-- Dune's curated cex.flows table is more reliable, but only covers EVM chains.
-- This query uses Ethereum CEX deposits/withdrawals as a cross-market exchange
-- pressure signal and converts net USD flow into BTC-equivalent units.

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
        CASE
            WHEN spent_btc = 0 THEN NULL
            ELSE lth_spent_btc / spent_btc
        END AS lth_spent_share,
        CASE
            WHEN spent_btc = 0 THEN NULL
            ELSE sth_spent_btc / spent_btc
        END AS sth_spent_share,
        CASE
            WHEN one_to_two_year_spent_btc = 0 THEN NULL
            ELSE one_week_spent_btc / one_to_two_year_spent_btc
        END AS spent_age_rhodl_ratio
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

evm_cex_flow_usd AS (
    SELECT
        DATE_TRUNC('day', block_time) AS timestamp,
        COUNT(*) AS exchange_flow_rows,
        SUM(CASE WHEN flow_type = 'Inflow' THEN amount_usd ELSE 0 END) AS exchange_inflow_usd,
        SUM(CASE WHEN flow_type = 'Outflow' THEN amount_usd ELSE 0 END) AS exchange_outflow_usd,
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
),

exchange_features AS (
    SELECT
        f.timestamp,
        CASE
            WHEN p.btc_price_usd = 0 THEN NULL
            ELSE f.exchange_net_flow_usd / p.btc_price_usd
        END AS exchange_net_flow,
        f.exchange_net_flow_usd,
        f.exchange_inflow_usd,
        f.exchange_outflow_usd,
        f.exchange_flow_rows,
        p.btc_price_usd
    FROM evm_cex_flow_usd f
    LEFT JOIN btc_price p
      ON f.timestamp = p.timestamp
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
