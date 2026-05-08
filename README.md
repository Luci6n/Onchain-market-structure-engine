# On-Chain Market Intelligence Pipeline

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

The five Dune indicators remain the base score. Available supplemental sources increase the max score dynamically, so the terminal and HTML report may show a score such as `+2 / +9` instead of `+2 / +5`.

## Data Quality Notes

When the terminal says `source=dune`, the report is using live Dune rows. Some indicators are live proxies rather than exact vendor-style metrics:

- `SOPR`: live Dune-derived proxy
- `RHODL`: live Dune-derived proxy
- `Exchange Flow`: live EVM CEX flow converted to BTC-equivalent units
- `LTH/STH`: live Bitcoin spent-age rotation from Dune
- `Miner Behaviour`: live Puell-style proxy from Dune

When the terminal says `source=demo`, the report is using deterministic demo data for local testing.

## Backtesting And Visuals

The HTML report includes:

- market state timeline
- score line chart
- indicator table with 30-point sparklines
- historical state distribution
- state transition count
- forward 7D return by state when yfinance price history is available

Machine-readable backtest output is saved to:

```text
reports/backtest_report.json
```

## Project Files

```text
Blockchain.py                         Main Python pipeline
sql/                                  Dune SQL templates and diagnostics
reports/onchain_report.html           Visual dashboard/report
reports/onchain_summary.csv           Daily classification history
reports/source_context.json           API/source status
reports/backtest_report.json          Backtest metrics
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
