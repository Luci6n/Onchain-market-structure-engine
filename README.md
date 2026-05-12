# On-Chain Market Intelligence Pipeline

<img width="1440" height="775" alt="Screenshot 2026-05-12 at 3 03 24 PM" src="https://github.com/user-attachments/assets/a032d0bd-4802-4592-9d75-e07ffe96043f" />


An institutional-style crypto research pipeline that combines macro liquidity, capital flow, on-chain behavior, narrative rotation, and a rule-based regime engine into one terminal report, HTML dashboard, and machine-readable JSON output.

This project follows the analysis flow:

```text
Dune (SQL)
  -> Python Pipeline
  -> Feature Extraction
  -> Rule-based Logic
  -> Market Classification
  -> Dashboard / Report
```

## Indicators

- SOPR
- RHODL
- Exchange Flow
- LTH/STH holder rotation
- Miner Behaviour via Puell Multiple

Each indicator contributes `-1`, `0`, or `+1`. The final market score maps to states such as `ACCUMULATION / STRONG BUY`, `BULLISH TREND`, `DISTRIBUTION`, or `BEARISH / RISK OFF`. The max score expands when supplemental sources are available.

## Run Locally

Run the full pipeline with deterministic demo data:

```bash
python3 Blockchain.py --demo --days 180
```

Run the live pipeline using `.env` API keys:

```bash
python3 Blockchain.py
```

Generated artifacts:

- `reports/onchain_summary.csv`
- `reports/onchain_report.html`
- `reports/source_context.json`
- `reports/backtest_report.json`
- `reports/regime_report.json`

Print the Dune SQL schema template:

```bash
python3 Blockchain.py --print-dune-sql
```

The upgraded Dune SQL is also saved at:

```text
sql/btc_classifier_lth_exchange.sql
```

Exchange-flow helpers:

```text
sql/exchange_flow_diagnostics.sql
sql/btc_classifier_lth_evm_exchange.sql
sql/btc_classifier_compact_evm_exchange.sql
sql/cex_flows_probe.sql
sql/evm_exchange_flow_only.sql
```

## Live Data Setup

Set the environment variables for the sources you want to enable. The app also auto-loads a local `.env` file.

```bash
export DUNE_API_KEY="..."
export DUNE_QUERY_ID="..."
export ETHERSCAN_API_KEY="..."
export CARDANOSCAN_API_KEY="..."
export SOLSCAN_API_KEY="..."
export MARKET_SYMBOL="BTC-USD"
```

Required Python packages for live Dune and market context:

```bash
pip install pandas numpy dune-client yfinance
```

Optional state calibration thresholds use score ratios:

```bash
export STATE_STRONG_BULLISH="0.70"
export STATE_BULLISH="0.35"
export STATE_MILD_BULLISH="0.15"
export STATE_MILD_BEARISH="-0.15"
export STATE_BEARISH="-0.35"
export STATE_STRONG_BEARISH="-0.70"
```

The Dune query must return these columns:

```text
timestamp
sopr
rhodl_ratio
exchange_net_flow
lth_supply_change_pct
sth_supply_change_pct
miner_puell_multiple
```

## Upgrading The Dune Query

Use `sql/btc_classifier_compact_evm_exchange.sql` for the current practical live classifier query. It keeps the BTC block and LTH/STH logic, uses Dune's curated Ethereum `cex.flows` table for Exchange Flow, and converts net USD flow into BTC-equivalent units.

Use `sql/btc_classifier_lth_exchange.sql` if you want best-effort native BTC exchange flow from `cex.addresses`. If BTC exchange flow stays zero, use `sql/exchange_flow_diagnostics.sql` to inspect whether Dune's Bitcoin CEX labels match `bitcoin.inputs.address` / `bitcoin.outputs.address`.

The Dune queries intentionally avoid the very expensive full UTXO cost-basis join that can exceed Dune cluster capacity. They use:

- `bitcoin.blocks` for fee pressure and Puell-style miner behaviour
- `bitcoin.inputs` for spent-age LTH/STH movement
- `cex.flows` for practical live exchange flow

## Source Roles

The report is organized into four layers:

- **Layer 1: Macro Liquidity** tracks US10Y, DXY, Fed Funds, M2, VIX, BTC/NASDAQ correlation, and stablecoin supply growth.
- **Layer 2: Capital Flow** tracks BTC ETF flow proxy, BTC dominance, exchange reserve pressure proxy, stablecoin rotation, and sector rotation.
- **Layer 3: On-chain** keeps the existing Dune-driven SOPR/RHODL/Exchange Flow/LTH-STH/Miner Behaviour model.
- **Layer 4: Market Regime Engine** combines Macro, Capital Flow, On-chain, and Narrative Strength into one regime read.

The front page uses a custom market ontology instead of generic labels:

```text
Liquidity Expansion
BTC-Led Risk-On
Early Rotation
Narrative Expansion
Exhaustion
Transition
Compression
```

The HTML report is intentionally structured as a main dashboard plus expandable details. The first screen focuses on conclusion, regime, confidence, key drivers, probability distribution, and heatmap. Rules, methodology, source status, and full indicator tables live in collapsible sections.

Interactive controls are embedded directly into the static HTML report:

- Heatmap timeframe filter: `60D`, `30D`, or `14D`
- Heatmap signal filter by indicator row
- Click-to-drill heatmap cells
- Scenario engine sliders for BTC dominance, stablecoin growth, macro impulse, and narrative strength
- Dynamic probability bars that update from the scenario assumptions

Layer 4 also includes contextual rule logic, for example:

```text
IF Macro Liquidity is Positive
AND Capital Flow is Positive
AND BTC Dominance is falling
THEN Narrative Expansion Probability Up
```

It also adds decision-support views:

- **Confidence Scoring** grades High/Medium/Low confidence using indicator alignment, historical similarity, volatility regime, and data coverage.
- **Regime Transition Probability** estimates the probability of the custom market ontology states.
- **Historical Outcome Explorer** shows the closest historical regime and its reference BTC forward 30D median/max/min outcome.
- **Narrative Rotation Heatmap** tracks AI, RWA, DeFi, and Meme category strength from CoinGecko sector rotation data when available.
- **Signal Heatmap** visualizes recent market-state and indicator-vote history.

The report compares the current layer vector against a small historical regime database:

```text
2019 Bear Recovery
2020 QE / Liquidity Expansion
2021 Alt Season
2022 Liquidity Collapse
2024 ETF Regime
```

The rule-based classifier scores the seven-column Dune feed:

```text
timestamp, sopr, rhodl_ratio, exchange_net_flow,
lth_supply_change_pct, sth_supply_change_pct, miner_puell_multiple
```

Other sources add supplemental votes when their data is available:

- `yfinance` adds BTC market momentum. A 7D return above `+4%` is bullish; below `-4%` is bearish.
- `Etherscan` adds ETH gas pressure when a usable gas oracle response is available.
- `Cardanoscan` adds Cardano block freshness when latest-block data includes a recognizable timestamp.
- `Solscan` adds Solana block freshness when latest-block data includes a recognizable timestamp.
- `FRED` supplies Fed Funds and M2 series through public CSV endpoints.
- `DeFiLlama` supplies stablecoin supply growth.
- `CoinGecko` supplies BTC dominance and sector/category rotation.

The five Dune indicators remain the base score. Available supplemental sources increase the max score dynamically, so the terminal and HTML report may show a score such as `+2 / +9` instead of `+2 / +5`.

## Methodology

The system is intentionally rule-based and explainable. Every signal keeps its source, value, vote, and rule text in the terminal output, HTML report, and JSON artifacts.

- **Layer scoring:** each available indicator contributes `-1`, `0`, or `+1`; layer ratios normalize the score by the available max score.
- **Market state classification:** the on-chain layer maps total score ratios into accumulation, bullish, neutral, distribution, and risk-off states.
- **Historical similarity:** the current macro/capital/on-chain/narrative vector is compared with historical templates using distance-based similarity.
- **Regime probabilities:** transition probabilities are heuristic scenario scores, not calibrated statistical forecasts.
- **Confidence scoring:** confidence blends indicator alignment, historical similarity, volatility regime, and data coverage.
- **Market ontology:** the headline state is resolved from the layer combination, not only the raw on-chain classifier label.
- **Proxy treatment:** proxy metrics are labeled directly in the report so weak or unavailable data is visible instead of hidden.

## Data Quality Notes

When the terminal says `source=dune`, the report is using live Dune rows. Some indicators are live proxies rather than exact vendor-style metrics:

- `SOPR`: live Dune-derived proxy
- `RHODL`: live Dune-derived proxy
- `Exchange Flow`: live EVM CEX flow converted to BTC-equivalent units
- `LTH/STH`: live Bitcoin spent-age rotation from Dune
- `Miner Behaviour`: live Puell-style proxy from Dune
- `BTC ETF Flow`: signed dollar-volume proxy from spot BTC ETF tickers, not issuer-reported ETF net creations/redemptions

When the terminal says `source=demo`, the report is using deterministic demo data for local testing.

## Limitations And Disclaimer

This project is designed for probabilistic market-state research, not deterministic price prediction or financial advice.

- Public APIs can rate-limit, change schemas, or return stale values.
- SOPR, RHODL, ETF flow, exchange reserve pressure, and some sector metrics are practical public-data proxies.
- The historical regime database is a reference framework and should not be treated as proof that future outcomes will repeat.
- Live quality depends on the saved Dune query returning the required classifier columns.
- Backtests are useful for sanity checks, but they do not remove forward-looking uncertainty.

## Backtesting And Visuals

The HTML report includes:

- executive market ontology dashboard
- regime wheel
- probability distribution bars
- key driver cards
- scenario engine controls
- heatmap timeframe/filter/drill-down controls
- contextual regime logic table
- historical regime database similarity table
- confidence scoring
- regime transition probability
- historical outcome explorer
- narrative rotation heatmap
- market heatmap for recent state and indicator vote history
- indicator table with 30-point sparklines
- historical state distribution
- state transition count
- forward 7D return by state when yfinance price history is available

Machine-readable backtest output is saved to:

```text
reports/backtest_report.json
reports/regime_report.json
```

## Project Files

```text
Blockchain.py                         Main Python pipeline
sql/                                  Dune SQL templates and diagnostics
reports/onchain_report.html           Visual dashboard/report
reports/onchain_summary.csv           Daily classification history
reports/source_context.json           API/source status
reports/backtest_report.json          Backtest metrics
reports/regime_report.json            Macro/capital/on-chain/regime layer metrics
```

## Typical Workflow

1. Paste `sql/btc_classifier_compact_evm_exchange.sql` into Dune.
2. Save and run the Dune query.
3. Put that query ID in `.env` as `DUNE_QUERY_ID`.
4. Run:

```bash
python3 Blockchain.py
```

5. Open:

```text
reports/onchain_report.html
```
