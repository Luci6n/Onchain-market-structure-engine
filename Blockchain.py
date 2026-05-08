"""
On-Chain Market Intelligence Pipeline
=====================================

Flow:
    Dune (SQL)
      -> Python Pipeline
      -> Feature Extraction
      -> Rule-based Logic
      -> Market Classification
      -> Dashboard / Report

Core indicators:
    - SOPR
    - RHODL
    - Exchange Flow
    - LTH/STH
    - Miner Behaviour

The pipeline is production-friendly but still runnable without API keys. When a
provider is not configured, the code falls back to deterministic demo data with
the same schema, so the feature extraction and classifier can be tested locally.
"""

from __future__ import annotations

import argparse
import contextlib
import html
import json
import os
import urllib.parse
import urllib.request
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable

import numpy as np
import pandas as pd


# ---------------------------------------------------------------------------
# STAGE 1 - DUNE SQL DEFINITIONS
# ---------------------------------------------------------------------------

DUNE_SQL_TEMPLATES: dict[str, str] = {
    "btc_onchain_cycle": """
-- Expected output columns:
-- timestamp, sopr, rhodl_ratio, exchange_net_flow,
-- lth_supply_change_pct, sth_supply_change_pct, miner_puell_multiple
--
-- Lightweight live Dune version. It avoids the expensive full UTXO
-- cost-basis join, but adds real nonzero LTH/STH movement from bitcoin.inputs
-- and best-effort exchange flow from Dune's CEX address labels.

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
""".strip()
}


# ---------------------------------------------------------------------------
# CONFIGURATION
# ---------------------------------------------------------------------------

DEFAULT_OUTPUT_DIR = Path("reports")


@dataclass(frozen=True)
class StateCalibration:
    """Score-ratio thresholds used to map rule votes into market states."""

    strong_bullish: float = 0.70
    bullish: float = 0.35
    mild_bullish: float = 0.15
    mild_bearish: float = -0.15
    bearish: float = -0.35
    strong_bearish: float = -0.70

    @classmethod
    def from_env(cls) -> "StateCalibration":
        return cls(
            strong_bullish=float(os.getenv("STATE_STRONG_BULLISH", "0.70")),
            bullish=float(os.getenv("STATE_BULLISH", "0.35")),
            mild_bullish=float(os.getenv("STATE_MILD_BULLISH", "0.15")),
            mild_bearish=float(os.getenv("STATE_MILD_BEARISH", "-0.15")),
            bearish=float(os.getenv("STATE_BEARISH", "-0.35")),
            strong_bearish=float(os.getenv("STATE_STRONG_BEARISH", "-0.70")),
        )


def load_dotenv(path: Path = Path(".env")) -> None:
    """
    Load simple KEY=VALUE pairs from .env without overwriting shell env vars.

    This keeps the project dependency-light while still supporting local API
    keys during development.
    """
    if not path.exists():
        return

    for raw_line in path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue

        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


@dataclass(frozen=True)
class SourceConfig:
    """API and runtime configuration loaded from environment variables."""

    dune_api_key: str | None = None
    dune_query_id: str | None = None
    etherscan_api_key: str | None = None
    cardanoscan_api_key: str | None = None
    solscan_api_key: str | None = None
    symbol: str = "BTC-USD"
    days: int = 180
    demo_seed: int = 42
    calibration: StateCalibration = field(default_factory=StateCalibration)

    @classmethod
    def from_env(cls) -> "SourceConfig":
        load_dotenv()
        return cls(
            dune_api_key=os.getenv("DUNE_API_KEY"),
            dune_query_id=os.getenv("DUNE_QUERY_ID"),
            etherscan_api_key=os.getenv("ETHERSCAN_API_KEY"),
            cardanoscan_api_key=os.getenv("CARDANOSCAN_API_KEY"),
            solscan_api_key=os.getenv("SOLSCAN_API_KEY"),
            symbol=os.getenv("MARKET_SYMBOL", "BTC-USD"),
            days=int(os.getenv("ONCHAIN_DAYS", "180")),
            demo_seed=int(os.getenv("ONCHAIN_DEMO_SEED", "42")),
            calibration=StateCalibration.from_env(),
        )


REQUIRED_COLUMNS = {
    "timestamp",
    "sopr",
    "rhodl_ratio",
    "exchange_net_flow",
    "lth_supply_change_pct",
    "sth_supply_change_pct",
    "miner_puell_multiple",
}


# ---------------------------------------------------------------------------
# STAGE 2 - PYTHON DATA PIPELINE
# ---------------------------------------------------------------------------

def fetch_dune_data(config: SourceConfig) -> pd.DataFrame:
    """
    Fetch the validated Dune query output.

    Required env vars:
        DUNE_API_KEY
        DUNE_QUERY_ID

    The Dune query must return REQUIRED_COLUMNS. Keep the SQL inside Dune so
    your analyst can iterate on schemas without changing Python.
    """
    raw = fetch_dune_raw_data(config)
    return normalize_onchain_frame(raw, source_name="dune")


def fetch_dune_raw_data(config: SourceConfig) -> pd.DataFrame:
    """Fetch raw rows from the configured Dune query without enforcing schema."""
    if not config.dune_api_key or not config.dune_query_id:
        raise RuntimeError("Set DUNE_API_KEY and DUNE_QUERY_ID to fetch Dune data.")

    try:
        from dune_client.client import DuneClient
    except ImportError as exc:
        raise RuntimeError("Install dune-client to fetch Dune data: pip install dune-client") from exc

    dune = DuneClient(api_key=config.dune_api_key)
    result = dune.get_latest_result(int(config.dune_query_id))
    rows = result.result.rows if hasattr(result, "result") else result["result"]["rows"]
    return pd.DataFrame(rows)


def fetch_yfinance_market_context(config: SourceConfig) -> pd.DataFrame:
    """
    Fetch market context with yfinance when installed.

    This data is not a replacement for on-chain metrics; it gives the report
    price, volume, and return context alongside the on-chain classifier.
    """
    try:
        import yfinance as yf
    except ImportError:
        return pd.DataFrame()

    try:
        with open(os.devnull, "w", encoding="utf-8") as devnull:
            with contextlib.redirect_stdout(devnull), contextlib.redirect_stderr(devnull):
                hist = yf.download(
                    config.symbol,
                    period=f"{max(config.days, 30)}d",
                    interval="1d",
                    auto_adjust=True,
                    progress=False,
                )
    except Exception:
        return pd.DataFrame()
    if hist.empty:
        return pd.DataFrame()

    if isinstance(hist.columns, pd.MultiIndex):
        hist.columns = hist.columns.get_level_values(0)

    hist = hist.reset_index()
    date_col = "Date" if "Date" in hist.columns else "Datetime"

    close = pd.Series(hist["Close"].to_numpy().ravel(), index=hist.index, dtype="float64")
    volume = pd.Series(hist["Volume"].to_numpy().ravel(), index=hist.index, dtype="float64")

    return pd.DataFrame(
        {
            "timestamp": pd.to_datetime(hist[date_col]).dt.tz_localize(None),
            "market_close": close,
            "market_volume": volume,
            "market_return_1d": close.pct_change(),
        }
    )


def summarize_market_context(market: pd.DataFrame, symbol: str) -> dict[str, Any]:
    if market.empty:
        return {"yfinance_status": "unavailable or not installed", "yfinance_symbol": symbol}

    latest = market.dropna(subset=["market_close"]).tail(1)
    if latest.empty:
        return {"yfinance_status": "fetched but no usable close price", "yfinance_symbol": symbol}

    row = latest.iloc[0]
    return {
        "yfinance_status": "used as market context",
        "yfinance_symbol": symbol,
        "yfinance_latest_date": pd.to_datetime(row["timestamp"]).date().isoformat(),
        "yfinance_latest_close": round(float(row["market_close"]), 2),
        "yfinance_latest_return_1d_pct": None
        if pd.isna(row.get("market_return_1d"))
        else round(float(row["market_return_1d"]) * 100, 2),
    }


def _fetch_json(url: str, headers: dict[str, str] | None = None, timeout: int = 20) -> dict[str, Any]:
    req = urllib.request.Request(url, headers=headers or {"User-Agent": "onchain-pipeline/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as response:
        return json.loads(response.read().decode("utf-8"))


def _safe_float(value: Any) -> float | None:
    try:
        if value is None or value == "":
            return None
        return float(value)
    except (TypeError, ValueError):
        return None


def _walk_values(obj: Any) -> list[Any]:
    if isinstance(obj, dict):
        values = list(obj.values())
        for value in obj.values():
            values.extend(_walk_values(value))
        return values
    if isinstance(obj, list):
        values = list(obj)
        for value in obj:
            values.extend(_walk_values(value))
        return values
    return []


def _extract_recent_timestamp(payload: Any) -> datetime | None:
    timestamp_keys = {"time", "timestamp", "block_time", "blocktime", "blockTime", "slotTime"}
    candidates: list[Any] = []

    if isinstance(payload, dict):
        for key, value in payload.items():
            if key in timestamp_keys:
                candidates.append(value)
        candidates.extend(value for value in _walk_values(payload) if isinstance(value, (int, float, str)))

    now = datetime.now(timezone.utc)
    for value in candidates:
        parsed: datetime | None = None
        if isinstance(value, (int, float)):
            raw = float(value)
            if raw > 1_000_000_000_000:
                raw = raw / 1000
            if 1_200_000_000 <= raw <= now.timestamp() + 86_400:
                parsed = datetime.fromtimestamp(raw, tz=timezone.utc)
        elif isinstance(value, str):
            try:
                parsed = pd.to_datetime(value, utc=True).to_pydatetime()
            except (TypeError, ValueError):
                parsed = None

        if parsed and parsed <= now + timedelta(days=1):
            return parsed

    return None


def fetch_etherscan_context(config: SourceConfig) -> dict[str, Any]:
    """
    Fetch lightweight Ethereum network context from Etherscan.

    This adapter intentionally stays separate from the BTC cycle classifier. Use
    it for dashboard context or to extend the rule engine for Ethereum-specific
    indicators later.
    """
    if not config.etherscan_api_key:
        return {}

    params = urllib.parse.urlencode(
        {
            "module": "stats",
            "action": "ethsupply2",
            "apikey": config.etherscan_api_key,
        }
    )
    supply_payload = _fetch_json(f"https://api.etherscan.io/api?{params}")

    gas_params = urllib.parse.urlencode(
        {
            "module": "gastracker",
            "action": "gasoracle",
            "apikey": config.etherscan_api_key,
        }
    )
    gas_payload = _fetch_json(f"https://api.etherscan.io/api?{gas_params}")
    gas_result = gas_payload.get("result") if isinstance(gas_payload, dict) else {}

    return {
        "etherscan_eth_supply": supply_payload.get("result"),
        "etherscan_gas_oracle": gas_result,
        "etherscan_safe_gas_gwei": _safe_float(gas_result.get("SafeGasPrice")) if isinstance(gas_result, dict) else None,
        "etherscan_propose_gas_gwei": _safe_float(gas_result.get("ProposeGasPrice")) if isinstance(gas_result, dict) else None,
    }


def fetch_cardanoscan_context(config: SourceConfig) -> dict[str, Any]:
    """
    Placeholder adapter for Cardanoscan-compatible API context.

    Cardanoscan API access and endpoints vary by plan. Keep this adapter as the
    integration boundary, then map successful responses into dashboard context.
    """
    if not config.cardanoscan_api_key:
        return {}

    headers = {"apiKey": config.cardanoscan_api_key, "User-Agent": "onchain-pipeline/1.0"}
    payload = _fetch_json("https://api.cardanoscan.io/api/v1/block/latest", headers=headers)
    return {
        "cardanoscan_status": "fetched latest block context",
        "cardanoscan_latest_block": payload,
    }


def fetch_solscan_context(config: SourceConfig) -> dict[str, Any]:
    """
    Placeholder adapter for Solscan context.

    Solscan's v2 API generally requires a token. This function records readiness
    without mixing SOL-specific metrics into the BTC rule set.
    """
    if not config.solscan_api_key:
        return {}

    headers = {"token": config.solscan_api_key, "User-Agent": "onchain-pipeline/1.0"}
    payload = _fetch_json("https://pro-api.solscan.io/v2.0/block/last", headers=headers)
    return {
        "solscan_status": "fetched latest block context",
        "solscan_latest_block": payload,
    }


def fetch_cross_chain_context(config: SourceConfig) -> dict[str, Any]:
    context: dict[str, Any] = {}
    for fetcher in (fetch_etherscan_context, fetch_cardanoscan_context, fetch_solscan_context):
        try:
            context.update(fetcher(config))
        except Exception as exc:  # Network context should not block the classifier.
            context[f"{fetcher.__name__}_error"] = str(exc)
    return context


def build_demo_onchain_data(config: SourceConfig) -> pd.DataFrame:
    """
    Deterministic demo data that resembles a market cycle.

    It lets the full pipeline run locally before Dune/yfinance/API keys are set.
    """
    rng = np.random.default_rng(config.demo_seed)
    n = max(config.days, 45)
    timestamps = pd.date_range(end=pd.Timestamp.utcnow().normalize().tz_localize(None), periods=n, freq="D")
    phase = np.linspace(0, 3.5 * np.pi, n)

    sopr = 1.02 + 0.055 * np.sin(phase) + rng.normal(0, 0.012, n)
    rhodl = 1050 + 760 * np.sin(phase - 0.7) + rng.normal(0, 120, n)
    exchange_net_flow = -1500 * np.sin(phase + 0.3) + rng.normal(0, 1750, n)
    lth_supply_change_pct = 0.0035 * np.sin(phase - 1.0) + rng.normal(0, 0.002, n)
    sth_supply_change_pct = -lth_supply_change_pct + rng.normal(0, 0.0015, n)
    miner_puell_multiple = 0.9 + 0.45 * np.sin(phase + 0.9) + rng.normal(0, 0.08, n)

    return pd.DataFrame(
        {
            "timestamp": timestamps,
            "sopr": np.clip(sopr, 0.88, 1.18),
            "rhodl_ratio": np.clip(rhodl, 250, 2800),
            "exchange_net_flow": exchange_net_flow,
            "lth_supply_change_pct": lth_supply_change_pct,
            "sth_supply_change_pct": sth_supply_change_pct,
            "miner_puell_multiple": np.clip(miner_puell_multiple, 0.25, 2.1),
        }
    )


def build_partial_dune_proxy(raw_dune: pd.DataFrame, config: SourceConfig, reason: str) -> pd.DataFrame:
    """
    Use Dune timestamps and available network columns while proxy-filling the
    missing classifier metrics.

    This is deliberately labeled as proxy data. Current Dune columns such as
    block_count and total_fees_btc prove the query is live, but they are not
    sufficient to calculate SOPR, RHODL, exchange flow, or LTH/STH directly.
    """
    if "timestamp" not in raw_dune.columns:
        raise ValueError(reason)

    partial = raw_dune.copy()
    partial["timestamp"] = pd.to_datetime(partial["timestamp"]).dt.tz_localize(None)
    partial = partial.sort_values("timestamp").dropna(subset=["timestamp"])
    if partial.empty:
        raise ValueError(reason)

    proxy_config = SourceConfig(
        dune_api_key=config.dune_api_key,
        dune_query_id=config.dune_query_id,
        etherscan_api_key=config.etherscan_api_key,
        cardanoscan_api_key=config.cardanoscan_api_key,
        solscan_api_key=config.solscan_api_key,
        symbol=config.symbol,
        days=len(partial),
        demo_seed=config.demo_seed,
        calibration=config.calibration,
    )
    completed = build_demo_onchain_data(proxy_config).iloc[-len(partial):].reset_index(drop=True)
    completed["timestamp"] = partial["timestamp"].reset_index(drop=True)

    for column in REQUIRED_COLUMNS - {"timestamp"}:
        if column in partial.columns:
            completed[column] = pd.to_numeric(partial[column], errors="coerce").reset_index(drop=True)

    if "total_fees_btc" in partial.columns and "miner_puell_multiple" not in raw_dune.columns:
        fees = pd.to_numeric(partial["total_fees_btc"], errors="coerce").reset_index(drop=True)
        baseline = fees.rolling(30, min_periods=7).median().bfill().ffill()
        fee_ratio = fees.div(baseline.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan)
        completed["miner_puell_multiple"] = fee_ratio.fillna(completed["miner_puell_multiple"]).clip(0.25, 2.1)

    passthrough_columns = [col for col in partial.columns if col not in completed.columns]
    for column in passthrough_columns:
        completed[f"dune_{column}"] = partial[column].reset_index(drop=True)

    completed["data_quality"] = "dune_partial_proxy"
    completed.attrs["dune_warning"] = reason
    completed.attrs["dune_columns"] = list(map(str, raw_dune.columns))
    return normalize_onchain_frame(completed, source_name="dune_partial_proxy")


def normalize_onchain_frame(df: pd.DataFrame, source_name: str) -> pd.DataFrame:
    missing = REQUIRED_COLUMNS - set(df.columns)
    if missing:
        missing_list = ", ".join(sorted(missing))
        available = ", ".join(map(str, df.columns)) or "no columns"
        raise ValueError(
            f"{source_name} data is missing required columns: {missing_list}. "
            f"Available columns: {available}"
        )

    normalized = df.copy()
    normalized["timestamp"] = pd.to_datetime(normalized["timestamp"]).dt.tz_localize(None)
    numeric_columns = sorted(REQUIRED_COLUMNS - {"timestamp"})
    for column in numeric_columns:
        normalized[column] = pd.to_numeric(normalized[column], errors="coerce")

    normalized = normalized.sort_values("timestamp").dropna(subset=list(REQUIRED_COLUMNS))
    if normalized.empty:
        raise ValueError(f"{source_name} data has no usable rows after normalization.")
    return normalized


def fetch_onchain_data(config: SourceConfig, prefer_live: bool = True) -> tuple[pd.DataFrame, str]:
    """
    Fetch Dune data first. If unavailable, return deterministic demo data.
    """
    if prefer_live:
        try:
            raw_dune = fetch_dune_raw_data(config)
            try:
                return normalize_onchain_frame(raw_dune, source_name="dune"), "dune"
            except ValueError as exc:
                proxy = build_partial_dune_proxy(raw_dune, config, reason=str(exc))
                print(f"[data] Dune query is live but incomplete, using partial Dune proxy data: {exc}")
                return proxy, "dune_partial_proxy"
        except Exception as exc:
            print(f"[data] Dune unavailable, using demo data: {exc}")

    return normalize_onchain_frame(build_demo_onchain_data(config), source_name="demo"), "demo"


def merge_market_context(onchain: pd.DataFrame, market: pd.DataFrame) -> pd.DataFrame:
    if market.empty:
        return onchain

    left = onchain.copy()
    left["date"] = pd.to_datetime(left["timestamp"]).dt.date
    right = market.copy()
    right["date"] = pd.to_datetime(right["timestamp"]).dt.date
    merged = left.merge(right.drop(columns=["timestamp"]), on="date", how="left")
    return merged.drop(columns=["date"])


# ---------------------------------------------------------------------------
# STAGE 3 - FEATURE EXTRACTION
# ---------------------------------------------------------------------------

def extract_features(df: pd.DataFrame, window: int = 7) -> pd.DataFrame:
    """
    Smooth noisy daily data and derive directional features for the classifier.
    """
    if window < 2:
        raise ValueError("window must be at least 2 days")

    featured = df.copy()
    featured["sopr_ma"] = featured["sopr"].rolling(window).mean()
    featured["sopr_momentum"] = featured["sopr_ma"].diff()

    featured["rhodl_ma"] = featured["rhodl_ratio"].rolling(window).mean()
    featured["rhodl_momentum"] = featured["rhodl_ma"].pct_change()

    featured["flow_ma"] = featured["exchange_net_flow"].rolling(window).mean()
    featured["flow_cumulative"] = featured["exchange_net_flow"].rolling(window).sum()

    featured["lth_ma"] = featured["lth_supply_change_pct"].rolling(window).mean()
    featured["sth_ma"] = featured["sth_supply_change_pct"].rolling(window).mean()
    featured["lth_sth_rotation"] = featured["lth_ma"] - featured["sth_ma"]

    featured["puell_ma"] = featured["miner_puell_multiple"].rolling(window).mean()

    if "market_close" in featured.columns:
        featured["price_ma"] = featured["market_close"].rolling(window).mean()
        featured["price_momentum"] = featured["market_close"].pct_change(window)

    featured["flag_sopr_capitulation"] = featured["sopr_ma"] < 1.0
    featured["flag_sopr_overheating"] = featured["sopr_ma"] > 1.05
    featured["flag_exchange_outflow"] = featured["flow_cumulative"] < -5000
    featured["flag_lth_accumulating"] = featured["lth_sth_rotation"] > 0.004
    featured["flag_miner_stress"] = featured["puell_ma"] < 0.6

    return featured.dropna(subset=["sopr_ma", "rhodl_ma", "flow_cumulative", "lth_sth_rotation", "puell_ma"])


# Backwards-compatible alias for the original script API.
extract_signals = extract_features


# ---------------------------------------------------------------------------
# STAGE 4 - RULE-BASED LOGIC AND MARKET CLASSIFICATION
# ---------------------------------------------------------------------------

@dataclass
class SignalResult:
    indicator: str
    raw_value: float
    signal: str
    score: int
    detail: str = ""
    source: str = "dune"


@dataclass
class ClassificationResult:
    timestamp: datetime
    signals: list[SignalResult] = field(default_factory=list)
    total_score: int = 0
    max_score: int = 5
    market_state: str = ""
    conviction: str = ""

    @property
    def bullish_count(self) -> int:
        return sum(1 for signal in self.signals if signal.score > 0)

    @property
    def bearish_count(self) -> int:
        return sum(1 for signal in self.signals if signal.score < 0)


def _score_sopr(row: pd.Series) -> SignalResult:
    val = float(row["sopr_ma"])
    momentum = float(row.get("sopr_momentum", 0.0))
    if val < 0.98:
        return SignalResult("SOPR (7D MA)", val, "Capitulation / realized losses", -1, "sopr_ma < 0.98")
    if val > 1.05 and momentum < 0:
        return SignalResult("SOPR (7D MA)", val, "Profit-taking cooling from overheated levels", -1, "sopr_ma > 1.05 and falling")
    if 1.00 <= val <= 1.05 and momentum >= 0:
        return SignalResult("SOPR (7D MA)", val, "Healthy realized profit regime", +1, "1.00 <= sopr_ma <= 1.05 and rising")
    return SignalResult("SOPR (7D MA)", val, "Neutral realized P/L regime", 0, "0.98 <= sopr_ma < 1.00 or sopr momentum mixed")


def _score_rhodl(row: pd.Series) -> SignalResult:
    val = float(row["rhodl_ma"])
    momentum = float(row.get("rhodl_momentum", 0.0))
    if val < 800:
        return SignalResult("RHODL Ratio (7D MA)", val, "Long-horizon accumulation zone", +1, "rhodl_ma < 800")
    if val > 1800 and momentum > 0:
        return SignalResult("RHODL Ratio (7D MA)", val, "Speculative late-cycle pressure", -1, "rhodl_ma > 1800 and rising")
    return SignalResult("RHODL Ratio (7D MA)", val, "Mid-cycle holder balance", 0, "800 <= rhodl_ma <= 1,800 or momentum not rising")


def _score_exchange_flow(row: pd.Series) -> SignalResult:
    val = float(row["flow_cumulative"])
    if val < -8000:
        return SignalResult("Exchange Flow (7D Sum)", val, "Strong exchange outflow / accumulation", +1, "7D net flow < -8,000")
    if val > 8000:
        return SignalResult("Exchange Flow (7D Sum)", val, "Strong exchange inflow / distribution", -1, "7D net flow > +8,000")
    return SignalResult("Exchange Flow (7D Sum)", val, "Balanced exchange pressure", 0, "-8,000 <= 7D net flow <= +8,000")


def _score_lth_sth(row: pd.Series) -> SignalResult:
    val = float(row["lth_sth_rotation"])
    if val > 0.006:
        return SignalResult("LTH/STH Rotation", val, "Long-term holders absorbing supply", +1, "lth_ma - sth_ma > 0.6%")
    if val < -0.006:
        return SignalResult("LTH/STH Rotation", val, "Short-term holders taking supply / LTH distribution", -1, "lth_ma - sth_ma < -0.6%")
    return SignalResult("LTH/STH Rotation", val, "Holder rotation is neutral", 0, "-0.6% <= lth_ma - sth_ma <= +0.6%")


def _score_miner_behaviour(row: pd.Series) -> SignalResult:
    val = float(row["puell_ma"])
    if val < 0.6:
        return SignalResult("Miner Behaviour (Puell 7D)", val, "Miner revenue stress / undervaluation", +1, "puell_ma < 0.6")
    if val > 1.35:
        return SignalResult("Miner Behaviour (Puell 7D)", val, "High miner revenue / cycle heat", -1, "puell_ma > 1.35")
    return SignalResult("Miner Behaviour (Puell 7D)", val, "Normal miner revenue regime", 0, "0.6 <= puell_ma <= 1.35")


def _score_yfinance_market(row: pd.Series) -> SignalResult | None:
    if "price_momentum" not in row or pd.isna(row.get("price_momentum")):
        return None

    val = float(row["price_momentum"])
    if val > 0.04:
        return SignalResult("yfinance BTC Momentum", val, "Price momentum confirms risk-on demand", +1, "7D return > +4%", "yfinance")
    if val < -0.04:
        return SignalResult("yfinance BTC Momentum", val, "Price momentum confirms risk-off pressure", -1, "7D return < -4%", "yfinance")
    return SignalResult("yfinance BTC Momentum", val, "Price momentum is neutral", 0, "-4% <= 7D return <= +4%", "yfinance")


def _score_etherscan_gas(context: dict[str, Any]) -> SignalResult | None:
    gas = _safe_float(context.get("etherscan_safe_gas_gwei") or context.get("etherscan_propose_gas_gwei"))
    if gas is None:
        return None

    if gas <= 15:
        return SignalResult("Etherscan Gas Pressure", gas, "Low ETH gas supports risk appetite", +1, "safe/propose gas <= 15 gwei", "etherscan")
    if gas >= 80:
        return SignalResult("Etherscan Gas Pressure", gas, "High ETH gas signals congestion / friction", -1, "safe/propose gas >= 80 gwei", "etherscan")
    return SignalResult("Etherscan Gas Pressure", gas, "ETH gas is normal", 0, "15 < gas < 80 gwei", "etherscan")


def _score_chain_freshness(context: dict[str, Any], key: str, label: str, source: str) -> SignalResult | None:
    payload = context.get(key)
    if not payload:
        return None

    timestamp = _extract_recent_timestamp(payload)
    if timestamp is None:
        return SignalResult(label, float("nan"), "Latest block fetched but timestamp was not detected", 0, "no recognizable block timestamp", source)

    age_minutes = (datetime.now(timezone.utc) - timestamp).total_seconds() / 60
    if age_minutes <= 10:
        return SignalResult(label, age_minutes, "Latest block is fresh", +1, "block age <= 10 minutes", source)
    if age_minutes >= 60:
        return SignalResult(label, age_minutes, "Latest block appears stale", -1, "block age >= 60 minutes", source)
    return SignalResult(label, age_minutes, "Latest block freshness is acceptable", 0, "10 < block age < 60 minutes", source)


def build_context_signals(context: dict[str, Any]) -> list[SignalResult]:
    signals: list[SignalResult | None] = [
        _score_etherscan_gas(context),
        _score_chain_freshness(context, "cardanoscan_latest_block", "Cardanoscan Block Freshness", "cardanoscan"),
        _score_chain_freshness(context, "solscan_latest_block", "Solscan Block Freshness", "solscan"),
    ]
    return [signal for signal in signals if signal is not None]


SCORERS: tuple[Callable[[pd.Series], SignalResult | None], ...] = (
    _score_sopr,
    _score_rhodl,
    _score_exchange_flow,
    _score_lth_sth,
    _score_miner_behaviour,
    _score_yfinance_market,
)


def resolve_market_state(
    score: int,
    max_score: int,
    calibration: StateCalibration | None = None,
) -> tuple[str, str]:
    if max_score <= 0:
        return "UNDEFINED", "LOW"

    calibration = calibration or StateCalibration()
    ratio = score / max_score
    if ratio >= calibration.strong_bullish:
        return "ACCUMULATION / STRONG BUY", "HIGH"
    if ratio >= calibration.bullish:
        return "BULLISH TREND", "MEDIUM"
    if ratio >= calibration.mild_bullish:
        return "MILD BULLISH", "LOW"
    if ratio > calibration.mild_bearish:
        return "NEUTRAL / SIDEWAYS", "LOW"
    if ratio > calibration.bearish:
        return "CAUTION / MILD BEARISH", "LOW"
    if ratio > calibration.strong_bearish:
        return "DISTRIBUTION", "MEDIUM"
    return "BEARISH / RISK OFF", "HIGH"


def classify_row(
    row: pd.Series,
    context: dict[str, Any] | None = None,
    calibration: StateCalibration | None = None,
) -> ClassificationResult:
    signals = [signal for scorer in SCORERS if (signal := scorer(row)) is not None]
    if context:
        signals.extend(build_context_signals(context))
    total = sum(signal.score for signal in signals)
    max_score = len(signals)
    state, conviction = resolve_market_state(total, max_score, calibration=calibration)
    return ClassificationResult(
        timestamp=pd.to_datetime(row["timestamp"]).to_pydatetime(),
        signals=signals,
        total_score=total,
        max_score=max_score,
        market_state=state,
        conviction=conviction,
    )


# ---------------------------------------------------------------------------
# STAGE 5 - DASHBOARD / REPORT
# ---------------------------------------------------------------------------

SCORE_LABEL = {1: "BULL", 0: "NEUT", -1: "BEAR"}
CONVICTION_MARK = {"LOW": ".", "MEDIUM": "*", "HIGH": "#"}


def _format_signal_value(signal: SignalResult) -> str:
    if "Flow" in signal.indicator:
        return f"{signal.raw_value:+,.0f}"
    if "LTH/STH" in signal.indicator:
        return f"{signal.raw_value * 100:+.2f}%"
    if "Momentum" in signal.indicator:
        return f"{signal.raw_value * 100:+.2f}%"
    if "Gas" in signal.indicator:
        return f"{signal.raw_value:.1f}"
    if "Freshness" in signal.indicator:
        if pd.isna(signal.raw_value):
            return "n/a"
        return f"{signal.raw_value:.1f}m"
    if "RHODL" in signal.indicator:
        return f"{signal.raw_value:,.0f}"
    return f"{signal.raw_value:.3f}"


def print_report(result: ClassificationResult, source: str = "unknown") -> None:
    width = 78
    print("=" * width)
    print(f"  ON-CHAIN MARKET REPORT | {result.timestamp.date()} | source={source}")
    print("=" * width)
    print(f"  Market State : {result.market_state}")
    print(f"  Score        : {result.total_score:+d} / +{result.max_score}")
    print(f"  Conviction   : {CONVICTION_MARK[result.conviction]} {result.conviction}")
    print(f"  Bulls / Bears: {result.bullish_count} bullish | {result.bearish_count} bearish")
    print("-" * width)
    print(f"  {'Source':<11} {'Indicator':<32} {'Value':>12}  {'Score':<5} Signal")
    print("-" * width)
    for signal in result.signals:
        print(
            f"  {signal.source:<11} "
            f"{signal.indicator:<32} "
            f"{_format_signal_value(signal):>12}  "
            f"{SCORE_LABEL[signal.score]:<5} {signal.signal}"
        )
    print("=" * width)


def build_summary_df(df: pd.DataFrame, calibration: StateCalibration | None = None) -> pd.DataFrame:
    records = []
    for _, row in df.iterrows():
        result = classify_row(row, calibration=calibration)
        record = {
            "date": result.timestamp.date().isoformat(),
            "score": result.total_score,
            "max_score": result.max_score,
            "score_ratio": result.total_score / result.max_score if result.max_score else 0,
            "market_state": result.market_state,
            "conviction": result.conviction,
            "sopr_ma": row["sopr_ma"],
            "rhodl_ma": row["rhodl_ma"],
            "exchange_flow_7d": row["flow_cumulative"],
            "lth_sth_rotation": row["lth_sth_rotation"],
            "puell_ma": row["puell_ma"],
        }
        if "data_quality" in row:
            record["data_quality"] = row.get("data_quality")
        for signal in result.signals:
            key = signal.indicator.lower().replace(" ", "_").replace("/", "_").replace("(", "").replace(")", "")
            record[f"{key}_score"] = signal.score
        if "market_close" in row:
            record["market_close"] = row.get("market_close")
            record["market_return_1d"] = row.get("market_return_1d")
        records.append(record)
    return pd.DataFrame(records)


STATE_COLORS = {
    "ACCUMULATION / STRONG BUY": "#087f5b",
    "BULLISH TREND": "#12b76a",
    "MILD BULLISH": "#84cc16",
    "NEUTRAL / SIDEWAYS": "#667085",
    "CAUTION / MILD BEARISH": "#f79009",
    "DISTRIBUTION": "#f04438",
    "BEARISH / RISK OFF": "#912018",
}


def build_backtest_report(summary: pd.DataFrame) -> dict[str, Any]:
    if summary.empty:
        return {"rows": 0}

    backtest = summary.copy()
    backtest["date"] = pd.to_datetime(backtest["date"])
    backtest["state_changed"] = backtest["market_state"].ne(backtest["market_state"].shift())
    distribution = backtest["market_state"].value_counts().to_dict()

    report: dict[str, Any] = {
        "rows": int(len(backtest)),
        "start_date": backtest["date"].min().date().isoformat(),
        "end_date": backtest["date"].max().date().isoformat(),
        "state_distribution": distribution,
        "state_transitions": int(backtest["state_changed"].sum()),
        "avg_score": round(float(backtest["score"].mean()), 3),
        "avg_score_ratio": round(float(backtest.get("score_ratio", pd.Series([0])).mean()), 3),
    }

    if "market_close" in backtest.columns and backtest["market_close"].notna().sum() > 10:
        backtest["forward_return_7d"] = backtest["market_close"].shift(-7) / backtest["market_close"] - 1
        state_returns = (
            backtest.dropna(subset=["forward_return_7d"])
            .groupby("market_state")["forward_return_7d"]
            .agg(["count", "mean", "median"])
            .reset_index()
        )
        report["forward_return_7d_by_state"] = [
            {
                "market_state": row["market_state"],
                "count": int(row["count"]),
                "mean_pct": round(float(row["mean"]) * 100, 2),
                "median_pct": round(float(row["median"]) * 100, 2),
            }
            for _, row in state_returns.iterrows()
        ]
    else:
        report["forward_return_7d_by_state"] = []

    return report


def _scale_points(values: list[float], width: int, height: int, pad: int = 14) -> str:
    clean = [0.0 if pd.isna(v) else float(v) for v in values]
    if not clean:
        return ""
    lo, hi = min(clean), max(clean)
    span = hi - lo if hi != lo else 1.0
    x_step = (width - pad * 2) / max(len(clean) - 1, 1)
    points = []
    for idx, value in enumerate(clean):
        x = pad + idx * x_step
        y = height - pad - ((value - lo) / span) * (height - pad * 2)
        points.append(f"{x:.1f},{y:.1f}")
    return " ".join(points)


def render_score_line_chart(summary: pd.DataFrame, width: int = 860, height: int = 220) -> str:
    chart = summary.tail(90)
    points = _scale_points(chart["score"].astype(float).tolist(), width, height)
    zero_y = height / 2
    return f"""
    <svg class="chart" viewBox="0 0 {width} {height}" role="img" aria-label="Score line chart">
      <line x1="14" y1="{zero_y:.1f}" x2="{width - 14}" y2="{zero_y:.1f}" class="axis-line" />
      <polyline points="{points}" class="score-line" />
    </svg>
    """


def render_market_state_timeline(summary: pd.DataFrame, width: int = 860, height: int = 74) -> str:
    timeline = summary.tail(90).reset_index(drop=True)
    if timeline.empty:
        return ""
    step = width / len(timeline)
    rects = []
    for idx, row in timeline.iterrows():
        color = STATE_COLORS.get(row["market_state"], "#667085")
        rects.append(
            f'<rect x="{idx * step:.2f}" y="8" width="{max(step, 1):.2f}" height="34" fill="{color}">'
            f'<title>{html.escape(str(row["date"]))}: {html.escape(str(row["market_state"]))}</title></rect>'
        )
    return f"""
    <svg class="timeline" viewBox="0 0 {width} {height}" role="img" aria-label="Market state timeline">
      {''.join(rects)}
      <text x="0" y="66">Older</text>
      <text x="{width}" y="66" text-anchor="end">Latest</text>
    </svg>
    """


def render_sparkline(values: pd.Series, width: int = 130, height: int = 34) -> str:
    points = _scale_points(pd.to_numeric(values, errors="coerce").tail(30).tolist(), width, height, pad=4)
    return f'<svg class="spark" viewBox="0 0 {width} {height}"><polyline points="{points}" /></svg>'


def render_indicator_spark_table(summary: pd.DataFrame) -> str:
    indicators = [
        ("SOPR", "sopr_ma", "{:.3f}"),
        ("RHODL", "rhodl_ma", "{:,.0f}"),
        ("Exchange Flow", "exchange_flow_7d", "{:+,.0f}"),
        ("LTH/STH", "lth_sth_rotation", "{:+.2%}"),
        ("Puell", "puell_ma", "{:.3f}"),
    ]
    rows = []
    latest = summary.tail(1).iloc[0] if not summary.empty else {}
    for name, column, fmt in indicators:
        if column not in summary.columns:
            continue
        value = latest.get(column, np.nan) if hasattr(latest, "get") else np.nan
        value_text = "n/a" if pd.isna(value) else fmt.format(float(value))
        rows.append(
            f"<tr><td>{html.escape(name)}</td><td>{value_text}</td><td>{render_sparkline(summary[column])}</td></tr>"
        )
    return "\n".join(rows)


def render_backtest_tables(backtest: dict[str, Any]) -> tuple[str, str]:
    distribution = backtest.get("state_distribution", {})
    distribution_rows = "\n".join(
        f"<tr><td>{html.escape(str(state))}</td><td>{count}</td></tr>"
        for state, count in distribution.items()
    )
    forward_rows = "\n".join(
        f"<tr><td>{html.escape(row['market_state'])}</td><td>{row['count']}</td><td>{row['mean_pct']:+.2f}%</td><td>{row['median_pct']:+.2f}%</td></tr>"
        for row in backtest.get("forward_return_7d_by_state", [])
    )
    if not forward_rows:
        forward_rows = '<tr><td colspan="4">Forward-return backtest needs yfinance market_close history.</td></tr>'
    return distribution_rows, forward_rows


def _html_cell(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def _compact_context_value(value: Any, max_len: int = 180) -> str:
    if isinstance(value, (dict, list)):
        text = json.dumps(value, default=str)
    else:
        text = str(value)
    return text if len(text) <= max_len else text[: max_len - 1] + "..."


def build_data_quality_notes(source: str, context: dict[str, Any]) -> list[str]:
    notes: list[str] = []
    if source == "demo":
        notes.append("Demo data is active. Run live with reachable Dune API to use real classifier rows.")
    elif source == "dune":
        notes.append("Dune classifier rows are live.")
    elif source == "dune_partial_proxy":
        notes.append("Dune is live but missing classifier columns; unavailable metrics were proxy-filled.")

    notes.extend(
        [
            "SOPR and RHODL are live Dune-derived proxies, not exact Glassnode-style cost-basis metrics.",
            "Exchange Flow uses live EVM CEX flow converted to BTC-equivalent units when the compact EVM query is saved in Dune.",
            "LTH/STH uses live Bitcoin spent-age rotation from Dune.",
        ]
    )

    if context.get("fetch_solscan_context_error"):
        notes.append("Solscan is not voting because the API call failed.")
    if context.get("fetch_etherscan_context_error") or context.get("etherscan_safe_gas_gwei") is None:
        notes.append("Etherscan gas is not voting unless a usable gas oracle response is available.")
    return notes


def write_html_report(
    latest: ClassificationResult,
    summary: pd.DataFrame,
    output_path: Path,
    source: str,
    context: dict[str, Any],
    backtest: dict[str, Any],
) -> None:
    latest_rows = "\n".join(
        f"""
        <tr>
            <td>{_html_cell(signal.source)}</td>
            <td>{_html_cell(signal.indicator)}</td>
            <td>{_html_cell(_format_signal_value(signal))}</td>
            <td class="score score-{signal.score}">{SCORE_LABEL[signal.score]}</td>
            <td>{_html_cell(signal.signal)}</td>
            <td class="rule-cell"><code>{_html_cell(signal.detail)}</code></td>
        </tr>
        """.strip()
        for signal in latest.signals
    )
    recent_rows = "\n".join(
        f"""
        <tr>
            <td>{row.date}</td>
            <td>{row.score:+d}</td>
            <td>{row.market_state}</td>
            <td>{row.conviction}</td>
        </tr>
        """.strip()
        for row in summary.tail(14).itertuples()
    )
    context_items = "\n".join(
        f"<li><b>{_html_cell(key)}</b>: {_html_cell(_compact_context_value(value))}</li>"
        for key, value in context.items()
    ) or "<li>No cross-chain context configured.</li>"
    score_chart = render_score_line_chart(summary)
    state_timeline = render_market_state_timeline(summary)
    spark_rows = render_indicator_spark_table(summary)
    distribution_rows, forward_rows = render_backtest_tables(backtest)
    quality_notes = "\n".join(f"<li>{_html_cell(note)}</li>" for note in build_data_quality_notes(source, context))

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>On-Chain Market Report</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #172026;
      --muted: #5c6873;
      --line: #d7dee5;
      --bg: #f7f9fb;
      --panel: #ffffff;
      --bull: #087f5b;
      --bear: #b42318;
      --neutral: #475467;
    }}
    body {{
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--ink);
    }}
    main {{
      width: min(1120px, calc(100% - 32px));
      margin: 28px auto 48px;
    }}
    header {{
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 16px;
      align-items: end;
      border-bottom: 1px solid var(--line);
      padding-bottom: 18px;
    }}
    h1, h2 {{ margin: 0; letter-spacing: 0; }}
    h1 {{ font-size: clamp(26px, 4vw, 44px); }}
    h2 {{ font-size: 18px; margin-top: 32px; margin-bottom: 12px; }}
    .meta {{ color: var(--muted); margin-top: 8px; }}
    .state {{
      text-align: right;
      min-width: 260px;
    }}
    .state strong {{
      display: block;
      font-size: 22px;
    }}
    .score-pill {{
      display: inline-block;
      margin-top: 8px;
      padding: 6px 10px;
      border: 1px solid var(--line);
      background: var(--panel);
      font-weight: 700;
    }}
    .quality {{
      margin: 18px 0 0;
      padding: 12px 14px;
      background: var(--panel);
      border: 1px solid var(--line);
    }}
    .quality ul {{
      margin: 8px 0 0;
    }}
    table {{
      width: 100%;
      border-collapse: collapse;
      background: var(--panel);
      border: 1px solid var(--line);
      table-layout: fixed;
    }}
    th, td {{
      padding: 10px 12px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
      font-size: 14px;
      overflow-wrap: anywhere;
    }}
    th {{ color: var(--muted); font-weight: 700; }}
    code {{
      font-size: 12px;
      background: #eef2f6;
      padding: 2px 4px;
      white-space: normal;
      overflow-wrap: anywhere;
      line-height: 1.5;
      display: inline;
    }}
    .rule-cell {{
      min-width: 150px;
    }}
    .score {{ font-weight: 800; }}
    .score-1 {{ color: var(--bull); }}
    .score-0 {{ color: var(--neutral); }}
    .score--1 {{ color: var(--bear); }}
    .chart-wrap {{
      display: grid;
      grid-template-columns: 1fr;
      gap: 18px;
      margin-top: 18px;
    }}
    .chart, .timeline {{
      width: 100%;
      background: var(--panel);
      border: 1px solid var(--line);
      display: block;
    }}
    .axis-line {{
      stroke: #d0d5dd;
      stroke-width: 1;
      stroke-dasharray: 4 4;
    }}
    .score-line {{
      fill: none;
      stroke: #175cd3;
      stroke-width: 3;
      stroke-linejoin: round;
      stroke-linecap: round;
    }}
    .spark {{
      width: 130px;
      height: 34px;
      display: block;
    }}
    .spark polyline {{
      fill: none;
      stroke: #175cd3;
      stroke-width: 2;
      stroke-linejoin: round;
      stroke-linecap: round;
    }}
    .kpis {{
      display: grid;
      grid-template-columns: repeat(4, minmax(0, 1fr));
      gap: 10px;
      margin-top: 18px;
    }}
    .kpi {{
      background: var(--panel);
      border: 1px solid var(--line);
      padding: 12px;
    }}
    .kpi b {{
      display: block;
      font-size: 20px;
      margin-top: 4px;
    }}
    ul {{
      margin: 0;
      padding-left: 20px;
      color: var(--muted);
    }}
    @media (max-width: 760px) {{
      header {{ grid-template-columns: 1fr; }}
      .kpis {{ grid-template-columns: 1fr 1fr; }}
      .state {{ text-align: left; min-width: 0; }}
      table {{ display: block; overflow-x: auto; }}
    }}
  </style>
</head>
<body>
  <main>
    <header>
      <div>
        <h1>On-Chain Market Report</h1>
        <div class="meta">Generated {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")} | data source: {source}</div>
      </div>
      <div class="state">
        <strong>{latest.market_state}</strong>
        <div class="score-pill">Score {latest.total_score:+d} / +{latest.max_score} | {latest.conviction}</div>
      </div>
    </header>

    <section class="quality">
      <strong>Data Quality Notes</strong>
      <ul>{quality_notes}</ul>
    </section>

    <h2>Latest Indicator Read</h2>
    <table>
      <thead>
        <tr><th>Source</th><th>Indicator</th><th>Value</th><th>Score</th><th>Signal</th><th>Rule</th></tr>
      </thead>
      <tbody>{latest_rows}</tbody>
    </table>

    <h2>Visuals</h2>
    <div class="chart-wrap">
      {state_timeline}
      {score_chart}
    </div>

    <h2>Indicator Sparklines</h2>
    <table>
      <thead>
        <tr><th>Indicator</th><th>Latest</th><th>30-Point Sparkline</th></tr>
      </thead>
      <tbody>{spark_rows}</tbody>
    </table>

    <h2>Backtest</h2>
    <div class="kpis">
      <div class="kpi">Rows<b>{backtest.get("rows", 0)}</b></div>
      <div class="kpi">Period<b>{backtest.get("start_date", "n/a")} to {backtest.get("end_date", "n/a")}</b></div>
      <div class="kpi">Transitions<b>{backtest.get("state_transitions", 0)}</b></div>
      <div class="kpi">Avg Score<b>{backtest.get("avg_score", 0)}</b></div>
    </div>

    <h2>State Distribution</h2>
    <table>
      <thead>
        <tr><th>Market State</th><th>Count</th></tr>
      </thead>
      <tbody>{distribution_rows}</tbody>
    </table>

    <h2>Forward Returns By State</h2>
    <table>
      <thead>
        <tr><th>Market State</th><th>Samples</th><th>Mean 7D Return</th><th>Median 7D Return</th></tr>
      </thead>
      <tbody>{forward_rows}</tbody>
    </table>

    <h2>Recent Classifications</h2>
    <table>
      <thead>
        <tr><th>Date</th><th>Score</th><th>Market State</th><th>Conviction</th></tr>
      </thead>
      <tbody>{recent_rows}</tbody>
    </table>

    <h2>Cross-Chain Source Status</h2>
    <ul>{context_items}</ul>
  </main>
</body>
</html>
"""
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(html, encoding="utf-8")


def export_artifacts(
    summary: pd.DataFrame,
    latest: ClassificationResult,
    output_dir: Path,
    source: str,
    context: dict[str, Any],
    backtest: dict[str, Any],
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "onchain_summary.csv"
    html_path = output_dir / "onchain_report.html"
    context_path = output_dir / "source_context.json"
    backtest_path = output_dir / "backtest_report.json"

    summary.to_csv(summary_path, index=False)
    write_html_report(latest, summary, html_path, source=source, context=context, backtest=backtest)
    context_path.write_text(json.dumps(context, indent=2, default=str), encoding="utf-8")
    backtest_path.write_text(json.dumps(backtest, indent=2, default=str), encoding="utf-8")
    return {
        "summary_csv": summary_path,
        "html_report": html_path,
        "source_context": context_path,
        "backtest_report": backtest_path,
    }


# ---------------------------------------------------------------------------
# ENTRYPOINT
# ---------------------------------------------------------------------------

def run_pipeline(
    config: SourceConfig | None = None,
    window: int = 7,
    prefer_live: bool = True,
    output_dir: Path = DEFAULT_OUTPUT_DIR,
    verbose: bool = True,
) -> tuple[pd.DataFrame, ClassificationResult]:
    """
    Execute the full flow and return daily classifications plus the latest read.
    """
    config = config or SourceConfig.from_env()
    raw, source = fetch_onchain_data(config, prefer_live=prefer_live)
    market_context = fetch_yfinance_market_context(config) if prefer_live else pd.DataFrame()
    merged = merge_market_context(raw, market_context)
    features = extract_features(merged, window=window)
    summary = build_summary_df(features, calibration=config.calibration)
    backtest = build_backtest_report(summary)

    cross_chain_context = fetch_cross_chain_context(config) if prefer_live else {}
    cross_chain_context.update(summarize_market_context(market_context, config.symbol))
    cross_chain_context["classifier_source_note"] = (
        "Base rule scores come from the Dune classifier schema. "
        "Supplemental rules can add votes from yfinance momentum, Etherscan gas pressure, "
        "Cardanoscan block freshness, and Solscan block freshness when those sources are available."
    )
    if raw.attrs.get("dune_warning"):
        cross_chain_context["dune_warning"] = raw.attrs["dune_warning"]
    if raw.attrs.get("dune_columns"):
        cross_chain_context["dune_available_columns"] = raw.attrs["dune_columns"]
    if source == "dune_partial_proxy":
        cross_chain_context["data_quality"] = (
            "Dune query is live, but missing one or more classifier indicators. "
            "Unavailable indicators were proxy-filled so the pipeline can run."
        )
    latest = classify_row(features.iloc[-1], context=cross_chain_context, calibration=config.calibration)
    artifacts = export_artifacts(summary, latest, output_dir, source=source, context=cross_chain_context, backtest=backtest)

    if verbose:
        print_report(latest, source=source)
        print()
        print("  14-day state distribution:")
        last14 = summary.tail(14)["market_state"].value_counts()
        for state, count in last14.items():
            print(f"    {count:>2}x  {state}")
        print()
        print("  Artifacts:")
        for label, path in artifacts.items():
            print(f"    {label}: {path}")
        print()

    return summary, latest


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run the on-chain market intelligence pipeline.")
    parser.add_argument("--demo", action="store_true", help="Use deterministic demo data instead of live APIs.")
    parser.add_argument("--window", type=int, default=7, help="Rolling feature window in days.")
    parser.add_argument("--days", type=int, default=None, help="Number of days to fetch/generate.")
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_OUTPUT_DIR, help="Directory for CSV/HTML artifacts.")
    parser.add_argument("--print-dune-sql", action="store_true", help="Print the Dune SQL template and exit.")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    if args.print_dune_sql:
        print(DUNE_SQL_TEMPLATES["btc_onchain_cycle"])
        raise SystemExit(0)

    env_config = SourceConfig.from_env()
    if args.days is not None:
        env_config = SourceConfig(
            dune_api_key=env_config.dune_api_key,
            dune_query_id=env_config.dune_query_id,
            etherscan_api_key=env_config.etherscan_api_key,
            cardanoscan_api_key=env_config.cardanoscan_api_key,
            solscan_api_key=env_config.solscan_api_key,
            symbol=env_config.symbol,
            days=args.days,
            demo_seed=env_config.demo_seed,
            calibration=env_config.calibration,
        )

    run_pipeline(
        config=env_config,
        window=args.window,
        prefer_live=not args.demo,
        output_dir=args.output_dir,
        verbose=True,
    )
