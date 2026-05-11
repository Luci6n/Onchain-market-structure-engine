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
import math
import os
import time
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
    dune_max_age_hours: int = 6
    dune_refresh_on_run: bool = True
    dune_refresh_timeout_seconds: int = 240
    dune_poll_seconds: int = 5
    stale_after_days: int = 1
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
            dune_max_age_hours=int(os.getenv("DUNE_MAX_AGE_HOURS", "6")),
            dune_refresh_on_run=os.getenv("DUNE_REFRESH_ON_RUN", "1").strip().lower() not in {"0", "false", "no"},
            dune_refresh_timeout_seconds=int(os.getenv("DUNE_REFRESH_TIMEOUT_SECONDS", "240")),
            dune_poll_seconds=int(os.getenv("DUNE_POLL_SECONDS", "5")),
            stale_after_days=int(os.getenv("ONCHAIN_STALE_AFTER_DAYS", "1")),
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


def _dune_api_json(
    config: SourceConfig,
    path: str,
    method: str = "GET",
    body: dict[str, Any] | None = None,
) -> dict[str, Any]:
    if not config.dune_api_key:
        raise RuntimeError("Set DUNE_API_KEY to fetch Dune data.")

    data = None if body is None else json.dumps(body).encode("utf-8")
    req = urllib.request.Request(
        f"https://api.dune.com/api/v1{path}",
        method=method,
        data=data,
        headers={
            "X-Dune-API-Key": config.dune_api_key,
            "Content-Type": "application/json",
            "User-Agent": "onchain-pipeline/1.0",
        },
    )
    with urllib.request.urlopen(req, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def fetch_dune_fresh_result(config: SourceConfig) -> pd.DataFrame:
    """Execute the saved Dune query and return fresh rows from the completed run."""
    if not config.dune_query_id:
        raise RuntimeError("Set DUNE_QUERY_ID to fetch Dune data.")

    execution = _dune_api_json(config, f"/query/{config.dune_query_id}/execute", method="POST", body={})
    execution_id = execution.get("execution_id")
    if not execution_id:
        raise RuntimeError(f"Dune execution did not return an execution_id: {execution}")

    deadline = time.monotonic() + config.dune_refresh_timeout_seconds
    status: dict[str, Any] = execution
    while time.monotonic() < deadline:
        status = _dune_api_json(config, f"/execution/{execution_id}/status")
        if status.get("is_execution_finished"):
            break
        time.sleep(max(config.dune_poll_seconds, 1))

    state = status.get("state")
    if not status.get("is_execution_finished"):
        raise TimeoutError(
            f"Dune execution {execution_id} did not finish within {config.dune_refresh_timeout_seconds}s; last state={state}"
        )
    if state != "QUERY_STATE_COMPLETED":
        raise RuntimeError(f"Dune execution {execution_id} finished with state={state}: {status}")

    result_payload = _dune_api_json(config, f"/execution/{execution_id}/results?limit=5000")
    rows = result_payload.get("result", {}).get("rows", [])
    frame = pd.DataFrame(rows)
    frame.attrs["dune_execution_id"] = execution_id
    frame.attrs["dune_execution_submitted_at"] = status.get("submitted_at")
    frame.attrs["dune_execution_ended_at"] = status.get("execution_ended_at")
    frame.attrs["dune_refresh_status"] = "fresh_execution"
    return frame


def fetch_dune_raw_data(config: SourceConfig) -> pd.DataFrame:
    """Fetch raw rows from the configured Dune query without enforcing schema."""
    if not config.dune_api_key or not config.dune_query_id:
        raise RuntimeError("Set DUNE_API_KEY and DUNE_QUERY_ID to fetch Dune data.")

    direct_refresh_error: str | None = None
    if config.dune_refresh_on_run:
        try:
            frame = fetch_dune_fresh_result(config)
            frame.attrs["dune_max_age_hours"] = config.dune_max_age_hours
            return frame
        except Exception as exc:
            direct_refresh_error = str(exc)

    try:
        from dune_client.client import DuneClient
    except ImportError as exc:
        raise RuntimeError("Install dune-client to fetch Dune data: pip install dune-client") from exc

    dune = DuneClient(api_key=config.dune_api_key)
    refresh_error: str | None = None
    try:
        result = dune.get_latest_result(int(config.dune_query_id), max_age_hours=config.dune_max_age_hours)
    except Exception as exc:
        refresh_error = str(exc)
        result = dune.get_latest_result(int(config.dune_query_id))

    rows = result.result.rows if hasattr(result, "result") else result["result"]["rows"]
    frame = pd.DataFrame(rows)

    times = getattr(result, "times", None)
    execution_id = getattr(result, "execution_id", None)
    if times is not None:
        ended_at = getattr(times, "execution_ended_at", None)
        submitted_at = getattr(times, "submitted_at", None)
        if ended_at:
            frame.attrs["dune_execution_ended_at"] = ended_at
        if submitted_at:
            frame.attrs["dune_execution_submitted_at"] = submitted_at
    if execution_id:
        frame.attrs["dune_execution_id"] = execution_id
    frame.attrs["dune_max_age_hours"] = config.dune_max_age_hours
    if direct_refresh_error:
        frame.attrs["dune_direct_refresh_warning"] = (
            f"Dune direct refresh failed, using cached query result instead: {direct_refresh_error}"
        )
    if refresh_error:
        frame.attrs["dune_refresh_warning"] = (
            f"Dune refresh failed, using last cached query result instead: {refresh_error}"
        )
    return frame


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

    base_url = "https://api.etherscan.io/v2/api"
    params = urllib.parse.urlencode(
        {
            "chainid": "1",
            "module": "stats",
            "action": "ethsupply2",
            "apikey": config.etherscan_api_key,
        }
    )
    supply_payload = _fetch_json(f"{base_url}?{params}")

    gas_params = urllib.parse.urlencode(
        {
            "chainid": "1",
            "module": "gastracker",
            "action": "gasoracle",
            "apikey": config.etherscan_api_key,
        }
    )
    gas_payload = _fetch_json(f"{base_url}?{gas_params}")
    gas_result = gas_payload.get("result") if isinstance(gas_payload, dict) else {}

    return {
        "etherscan_api_version": "v2",
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

    url = "https://pro-api.solscan.io/v2.0/block/last"
    header_options = (
        {"token": config.solscan_api_key, "User-Agent": "onchain-pipeline/1.0"},
        {"Authorization": f"Bearer {config.solscan_api_key}", "User-Agent": "onchain-pipeline/1.0"},
        {"X-API-KEY": config.solscan_api_key, "User-Agent": "onchain-pipeline/1.0"},
    )
    errors: list[str] = []
    payload: dict[str, Any] | None = None
    for headers in header_options:
        try:
            payload = _fetch_json(url, headers=headers)
            break
        except Exception as exc:
            errors.append(str(exc))

    if payload is None:
        raise RuntimeError("; ".join(errors))

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
        dune_max_age_hours=config.dune_max_age_hours,
        dune_refresh_on_run=config.dune_refresh_on_run,
        dune_refresh_timeout_seconds=config.dune_refresh_timeout_seconds,
        dune_poll_seconds=config.dune_poll_seconds,
        stale_after_days=config.stale_after_days,
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


def _latest_frame_date(frame: pd.DataFrame) -> pd.Timestamp | None:
    if frame.empty or "timestamp" not in frame.columns:
        return None

    timestamps = pd.to_datetime(frame["timestamp"], errors="coerce")
    if timestamps.dropna().empty:
        return None
    return timestamps.max()


def annotate_onchain_freshness(frame: pd.DataFrame, source: str, config: SourceConfig) -> None:
    latest_timestamp = _latest_frame_date(frame)
    if latest_timestamp is None:
        frame.attrs["onchain_freshness_status"] = "unknown"
        frame.attrs["onchain_freshness_warning"] = "No usable timestamp was found in the on-chain rows."
        return

    latest_date = latest_timestamp.date()
    today = datetime.now(timezone.utc).date()
    age_days = max((today - latest_date).days, 0)
    frame.attrs["onchain_latest_date"] = latest_date.isoformat()
    frame.attrs["onchain_row_age_days"] = age_days
    frame.attrs["onchain_freshness_status"] = "fresh" if age_days <= config.stale_after_days else "stale"

    if age_days > config.stale_after_days:
        frame.attrs["onchain_freshness_warning"] = (
            f"{source} latest on-chain row is {latest_date.isoformat()}, "
            f"{age_days} days behind current UTC date {today.isoformat()}."
        )


def fetch_onchain_data(config: SourceConfig, prefer_live: bool = True) -> tuple[pd.DataFrame, str]:
    """
    Fetch Dune data first. If unavailable, return deterministic demo data.
    """
    if prefer_live:
        try:
            raw_dune = fetch_dune_raw_data(config)
            try:
                normalized = normalize_onchain_frame(raw_dune, source_name="dune")
                annotate_onchain_freshness(normalized, "Dune", config)
                return normalized, "dune"
            except ValueError as exc:
                proxy = build_partial_dune_proxy(raw_dune, config, reason=str(exc))
                annotate_onchain_freshness(proxy, "Dune partial proxy", config)
                print(f"[data] Dune query is live but incomplete, using partial Dune proxy data: {exc}")
                return proxy, "dune_partial_proxy"
        except Exception as exc:
            print(f"[data] Dune unavailable, using demo data: {exc}")

    demo = normalize_onchain_frame(build_demo_onchain_data(config), source_name="demo")
    annotate_onchain_freshness(demo, "Demo", config)
    return demo, "demo"


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


@dataclass
class LayerSignal:
    indicator: str
    value: str
    score: int
    signal: str
    detail: str
    source: str


@dataclass
class LayerResult:
    layer: str
    score: int = 0
    max_score: int = 0
    status: str = "Unavailable"
    signals: list[LayerSignal] = field(default_factory=list)
    metadata: dict[str, Any] = field(default_factory=dict)

    @property
    def ratio(self) -> float:
        return self.score / self.max_score if self.max_score else 0.0


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
# STAGE 4B - MACRO / CAPITAL FLOW / REGIME LAYERS
# ---------------------------------------------------------------------------

def _resolve_simple_status(score: int, max_score: int, positive: str = "Positive", negative: str = "Negative") -> str:
    if max_score <= 0:
        return "Unavailable"
    ratio = score / max_score
    if ratio >= 0.20:
        return positive
    if ratio <= -0.20:
        return negative
    return "Neutral"


def _score_from_threshold(value: float | None, bullish_when: str, bull: float, bear: float) -> int:
    if value is None or pd.isna(value):
        return 0
    if bullish_when == "high":
        if value >= bull:
            return 1
        if value <= bear:
            return -1
    else:
        if value <= bull:
            return 1
        if value >= bear:
            return -1
    return 0


def _status_text(score: int, bull: str, bear: str, neutral: str = "Neutral") -> str:
    if score > 0:
        return bull
    if score < 0:
        return bear
    return neutral


def fetch_yfinance_close_history(ticker: str, days: int = 120) -> pd.Series:
    hist = fetch_yfinance_history(ticker, days=days)
    if hist.empty or "Close" not in hist:
        return pd.Series(dtype="float64")
    return pd.to_numeric(hist["Close"], errors="coerce").dropna()


def fetch_yfinance_history(ticker: str, days: int = 120) -> pd.DataFrame:
    try:
        import yfinance as yf
    except ImportError:
        return pd.DataFrame()

    try:
        with open(os.devnull, "w", encoding="utf-8") as devnull:
            with contextlib.redirect_stdout(devnull), contextlib.redirect_stderr(devnull):
                hist = yf.download(ticker, period=f"{days}d", interval="1d", auto_adjust=True, progress=False)
    except Exception:
        return pd.DataFrame()
    if hist.empty:
        return pd.DataFrame()
    if isinstance(hist.columns, pd.MultiIndex):
        hist.columns = hist.columns.get_level_values(0)
    return hist


def _series_return(series: pd.Series, periods: int = 20) -> float | None:
    clean = pd.to_numeric(series, errors="coerce").dropna()
    if len(clean) <= periods:
        return None
    start = float(clean.iloc[-periods - 1])
    end = float(clean.iloc[-1])
    if start == 0:
        return None
    return end / start - 1


def fetch_fred_series(series_id: str) -> pd.Series:
    url = f"https://fred.stlouisfed.org/graph/fredgraph.csv?id={urllib.parse.quote(series_id)}"
    try:
        with urllib.request.urlopen(url, timeout=20) as response:
            df = pd.read_csv(response)
    except Exception:
        return pd.Series(dtype="float64")
    if series_id not in df.columns:
        return pd.Series(dtype="float64")
    values = pd.to_numeric(df[series_id].replace(".", np.nan), errors="coerce")
    values.index = pd.to_datetime(df["observation_date"], errors="coerce")
    return values.dropna()


def fetch_defillama_stablecoins() -> dict[str, Any]:
    return _fetch_json("https://stablecoins.llama.fi/stablecoins?includePrices=true", timeout=25)


def _stable_total(payload: dict[str, Any], key: str = "circulating") -> float | None:
    total = 0.0
    found = False
    for asset in payload.get("peggedAssets", []):
        value = asset.get(key, {})
        if isinstance(value, dict):
            amount = _safe_float(value.get("peggedUSD"))
        else:
            amount = _safe_float(value)
        if amount is not None:
            total += amount
            found = True
    return total if found else None


def fetch_coingecko_global() -> dict[str, Any]:
    return _fetch_json("https://api.coingecko.com/api/v3/global", timeout=20)


def fetch_coingecko_categories() -> list[dict[str, Any]]:
    payload = _fetch_json("https://api.coingecko.com/api/v3/coins/categories", timeout=25)
    return payload if isinstance(payload, list) else []


def _add_layer_signal(signals: list[LayerSignal], indicator: str, value: str, score: int, signal: str, detail: str, source: str) -> None:
    signals.append(LayerSignal(indicator, value, score, signal, detail, source))


def build_macro_liquidity_layer(prefer_live: bool = True) -> LayerResult:
    if not prefer_live:
        return LayerResult("Macro Liquidity")

    signals: list[LayerSignal] = []

    us10y_ret = _series_return(fetch_yfinance_close_history("^TNX", 90), 20)
    score = _score_from_threshold(us10y_ret, "low", -0.02, 0.02)
    _add_layer_signal(signals, "US10Y", "n/a" if us10y_ret is None else f"{us10y_ret:+.2%}", score, _status_text(score, "Yields easing", "Yields tightening"), "20D change; lower yields support liquidity", "yfinance")

    dxy_ret = _series_return(fetch_yfinance_close_history("DX-Y.NYB", 90), 20)
    score = _score_from_threshold(dxy_ret, "low", -0.01, 0.01)
    _add_layer_signal(signals, "DXY", "n/a" if dxy_ret is None else f"{dxy_ret:+.2%}", score, _status_text(score, "Dollar weakening", "Dollar strengthening"), "20D change; weaker USD is crypto-liquidity friendly", "yfinance")

    fed = fetch_fred_series("FEDFUNDS")
    fed_delta = None if len(fed) < 4 else float(fed.iloc[-1] - fed.iloc[-4])
    score = _score_from_threshold(fed_delta, "low", -0.05, 0.05)
    _add_layer_signal(signals, "Fed Rate", "n/a" if fed_delta is None else f"{fed.iloc[-1]:.2f}% ({fed_delta:+.2f})", score, _status_text(score, "Policy easing", "Policy tightening"), "latest minus 3 observations", "FRED")

    m2 = fetch_fred_series("M2SL")
    m2_growth = None if len(m2) < 4 else float(m2.iloc[-1] / m2.iloc[-4] - 1)
    score = _score_from_threshold(m2_growth, "high", 0.01, -0.01)
    _add_layer_signal(signals, "M2", "n/a" if m2_growth is None else f"{m2_growth:+.2%}", score, _status_text(score, "Money supply expanding", "Money supply contracting"), "3-observation growth", "FRED")

    vix = fetch_yfinance_close_history("^VIX", 45)
    vix_latest = None if vix.empty else float(vix.iloc[-1])
    score = _score_from_threshold(vix_latest, "low", 18, 25)
    _add_layer_signal(signals, "VIX", "n/a" if vix_latest is None else f"{vix_latest:.1f}", score, _status_text(score, "Volatility calm", "Volatility elevated"), "VIX < 18 bullish, > 25 bearish", "yfinance")

    btc = fetch_yfinance_close_history("BTC-USD", 120)
    nasdaq = fetch_yfinance_close_history("^IXIC", 120)
    corr = None
    nasdaq_ret = _series_return(nasdaq, 20)
    if len(btc) > 35 and len(nasdaq) > 35:
        aligned = pd.concat([btc.pct_change(), nasdaq.pct_change()], axis=1).dropna().tail(30)
        if len(aligned) >= 20:
            corr = float(aligned.iloc[:, 0].corr(aligned.iloc[:, 1]))
    score = 0
    if corr is not None and nasdaq_ret is not None and corr > 0.35:
        score = 1 if nasdaq_ret > 0.03 else -1 if nasdaq_ret < -0.03 else 0
    _add_layer_signal(signals, "NASDAQ Correlation", "n/a" if corr is None else f"{corr:.2f}", score, _status_text(score, "Risk beta supportive", "Risk beta under pressure"), "30D BTC/NASDAQ return correlation plus NASDAQ trend", "yfinance")

    stable_growth = None
    try:
        stable_payload = fetch_defillama_stablecoins()
        current = _stable_total(stable_payload, "circulating")
        prev_month = _stable_total(stable_payload, "circulatingPrevMonth")
        if current and prev_month:
            stable_growth = current / prev_month - 1
    except Exception:
        stable_growth = None
    score = _score_from_threshold(stable_growth, "high", 0.01, -0.01)
    _add_layer_signal(signals, "Stablecoin Supply Growth", "n/a" if stable_growth is None else f"{stable_growth:+.2%}", score, _status_text(score, "Stablecoin liquidity expanding", "Stablecoin liquidity contracting"), "30D supply growth from DeFiLlama", "DeFiLlama")

    layer_score = sum(signal.score for signal in signals)
    return LayerResult("Macro Liquidity", layer_score, len(signals), _resolve_simple_status(layer_score, len(signals)), signals)


def build_capital_flow_layer(summary: pd.DataFrame, prefer_live: bool = True) -> LayerResult:
    if not prefer_live:
        return LayerResult("Capital Flow")

    signals: list[LayerSignal] = []

    etf_tickers = ["IBIT", "FBTC", "ARKB", "BITB", "GBTC"]
    etf_proxy = 0.0
    etf_found = False
    for ticker in etf_tickers:
        hist = fetch_yfinance_history(ticker, 45)
        if len(hist) > 6 and {"Close", "Volume"}.issubset(hist.columns):
            close = pd.to_numeric(hist["Close"], errors="coerce")
            volume = pd.to_numeric(hist["Volume"], errors="coerce")
            direction = np.sign(close.pct_change()).tail(5)
            dollar_proxy = (close.tail(5) * volume.tail(5) * direction.fillna(0)).sum()
            if not pd.isna(dollar_proxy) and dollar_proxy != 0:
                etf_proxy += float(dollar_proxy)
                etf_found = True
    score = _score_from_threshold(etf_proxy if etf_found else None, "high", 500_000_000, -500_000_000)
    _add_layer_signal(signals, "BTC ETF Flow Proxy", "n/a" if not etf_found else f"${etf_proxy:+,.0f}", score, _status_text(score, "ETF bid supportive", "ETF pressure negative"), "5D signed dollar-volume proxy from spot BTC ETF tickers", "yfinance")

    btc_dom = None
    try:
        global_payload = fetch_coingecko_global()
        btc_dom = _safe_float(global_payload.get("data", {}).get("market_cap_percentage", {}).get("btc"))
    except Exception:
        btc_dom = None
    score = 0
    if btc_dom is not None:
        score = 1 if btc_dom < 50 else -1 if btc_dom > 58 else 0
    _add_layer_signal(signals, "BTC Dominance", "n/a" if btc_dom is None else f"{btc_dom:.1f}%", score, _status_text(score, "Alt liquidity broadening", "BTC dominance defensive"), "BTC dominance <50 bullish for broad risk, >58 defensive", "CoinGecko")

    flow_latest = None
    if not summary.empty and "exchange_flow_7d" in summary.columns:
        flow_latest = _safe_float(summary["exchange_flow_7d"].iloc[-1])
    score = _score_from_threshold(flow_latest, "low", -8000, 8000)
    _add_layer_signal(signals, "Exchange Reserve Proxy", "n/a" if flow_latest is None else f"{flow_latest:+,.0f}", score, _status_text(score, "Exchange balances pressured lower", "Exchange inflow pressure"), "Uses 7D exchange net flow as reserve-pressure proxy", "Dune")

    stable_rotation = None
    try:
        stable_payload = fetch_defillama_stablecoins()
        current = _stable_total(stable_payload, "circulating")
        prev_month = _stable_total(stable_payload, "circulatingPrevMonth")
        if current and prev_month:
            stable_rotation = current / prev_month - 1
    except Exception:
        stable_rotation = None
    score = _score_from_threshold(stable_rotation, "high", 0.01, -0.01)
    _add_layer_signal(signals, "Stablecoin Rotation", "n/a" if stable_rotation is None else f"{stable_rotation:+.2%}", score, _status_text(score, "Dry powder expanding", "Stablecoin base shrinking"), "30D stablecoin supply growth as rotation proxy", "DeFiLlama")

    sector_strength = fetch_sector_rotation_strength()
    score = _score_from_threshold(sector_strength.get("average_change_pct"), "high", 2.0, -2.0)
    _add_layer_signal(signals, "Sector Rotation", sector_strength.get("display", "n/a"), score, _status_text(score, "Narratives attracting capital", "Narratives losing capital"), "Average 24H category market-cap change across AI/RWA/DeFi/Meme", "CoinGecko")

    layer_score = sum(signal.score for signal in signals)
    return LayerResult(
        "Capital Flow",
        layer_score,
        len(signals),
        _resolve_simple_status(layer_score, len(signals)),
        signals,
        {"sector_rotation": sector_strength},
    )


def narrative_strength_label(change_pct: float | None) -> str:
    if change_pct is None:
        return "Unavailable"
    if change_pct >= 2.0:
        return "Improving"
    if change_pct >= 0.5:
        return "Firming"
    if change_pct > -0.5:
        return "Neutral"
    if change_pct > -2.0:
        return "Weakening"
    return "Exhausted"


def fetch_sector_rotation_strength() -> dict[str, Any]:
    try:
        categories = fetch_coingecko_categories()
    except Exception:
        return {"display": "n/a", "average_change_pct": None, "leaders": [], "narratives": []}

    targets = {
        "AI": ["artificial intelligence", "ai ", "ai-"],
        "RWA": ["real world", "rwa"],
        "DeFi": ["defi", "decentralized finance"],
        "Meme": ["meme"],
    }
    picked: dict[str, dict[str, Any]] = {}
    for category in categories:
        name = str(category.get("name", "")).lower()
        category_id = str(category.get("id", "")).lower()
        haystack = f"{name} {category_id}"
        for key, needles in targets.items():
            if key not in picked and any(needle in haystack for needle in needles):
                picked[key] = category

    changes: list[float] = []
    leaders: list[str] = []
    narratives: list[dict[str, Any]] = []
    for key, category in picked.items():
        change = _safe_float(category.get("market_cap_change_24h"))
        if change is not None:
            changes.append(change)
            leaders.append(f"{key} {change:+.1f}%")
            narratives.append({"narrative": key, "change_pct": round(change, 2), "strength": narrative_strength_label(change)})

    avg = float(np.mean(changes)) if changes else None
    return {
        "display": "n/a" if avg is None else f"{avg:+.2f}% avg ({', '.join(leaders)})",
        "average_change_pct": avg,
        "leaders": leaders,
        "narratives": narratives,
    }


def build_narrative_strength_layer(capital_layer: LayerResult) -> LayerResult:
    sector = next((signal for signal in capital_layer.signals if signal.indicator == "Sector Rotation"), None)
    if sector is None:
        return LayerResult("Narrative Strength")
    status = "Strong" if sector.score > 0 else "Weak" if sector.score < 0 else "Neutral"
    return LayerResult("Narrative Strength", sector.score, 1, status, [sector], capital_layer.metadata)


def build_onchain_layer_result(latest: ClassificationResult) -> LayerResult:
    status = "Bullish" if latest.total_score / max(latest.max_score, 1) >= 0.15 else "Bearish" if latest.total_score / max(latest.max_score, 1) <= -0.15 else "Neutral"
    signals = [
        LayerSignal(signal.indicator, _format_signal_value(signal), signal.score, signal.signal, signal.detail, signal.source)
        for signal in latest.signals
        if signal.source == "dune"
    ]
    return LayerResult("On-chain", latest.total_score, latest.max_score, status, signals)


HISTORICAL_REGIME_DATABASE: list[dict[str, Any]] = [
    {
        "period": "2019 Bear Recovery",
        "macro": 0.10,
        "capital": -0.10,
        "onchain": 0.35,
        "narrative": -0.20,
        "regime": "Early Risk-On",
        "btc_forward_30d_median": 12.0,
        "btc_forward_30d_max": 38.0,
        "btc_forward_30d_min": -6.0,
        "notes": "Post-capitulation recovery with improving on-chain structure before broad liquidity confirmation.",
    },
    {
        "period": "2020 QE / Liquidity Expansion",
        "macro": 0.80,
        "capital": 0.45,
        "onchain": 0.35,
        "narrative": 0.20,
        "regime": "Risk-On",
        "btc_forward_30d_median": 18.0,
        "btc_forward_30d_max": 45.0,
        "btc_forward_30d_min": -8.0,
        "notes": "Macro liquidity impulse dominated; risk assets benefited from easing financial conditions.",
    },
    {
        "period": "2021 Alt Season",
        "macro": 0.45,
        "capital": 0.70,
        "onchain": 0.50,
        "narrative": 0.85,
        "regime": "Alt Risk-On",
        "btc_forward_30d_median": 10.0,
        "btc_forward_30d_max": 32.0,
        "btc_forward_30d_min": -18.0,
        "notes": "Broad capital rotation and strong narratives outperformed BTC-led exposure.",
    },
    {
        "period": "2022 Liquidity Collapse",
        "macro": -0.80,
        "capital": -0.65,
        "onchain": -0.55,
        "narrative": -0.75,
        "regime": "Risk-Off",
        "btc_forward_30d_median": -14.0,
        "btc_forward_30d_max": 8.0,
        "btc_forward_30d_min": -35.0,
        "notes": "Tightening liquidity, negative capital flow, weak narratives, and deteriorating on-chain structure.",
    },
    {
        "period": "2024 ETF Regime",
        "macro": 0.10,
        "capital": 0.70,
        "onchain": 0.30,
        "narrative": 0.25,
        "regime": "BTC-Led Risk-On",
        "btc_forward_30d_median": 9.0,
        "btc_forward_30d_max": 28.0,
        "btc_forward_30d_min": -10.0,
        "notes": "ETF-led demand and BTC dominance mattered more than broad alt rotation.",
    },
]


def build_contextual_regime_rules(macro: LayerResult, capital: LayerResult, onchain: LayerResult, narrative: LayerResult) -> list[dict[str, Any]]:
    rules: list[dict[str, Any]] = []
    layer_map = {layer.layer: layer for layer in (macro, capital, onchain, narrative)}

    def add_rule(name: str, condition: bool, result: str, explanation: str) -> None:
        rules.append(
            {
                "rule": name,
                "triggered": condition,
                "result": result if condition else "Not triggered",
                "explanation": explanation,
            }
        )

    btc_dom_signal = next((signal for signal in capital.signals if signal.indicator == "BTC Dominance"), None)
    etf_signal = next((signal for signal in capital.signals if signal.indicator == "BTC ETF Flow Proxy"), None)

    add_rule(
        "Alt Risk-On Probability",
        macro.status == "Positive" and capital.status == "Positive" and btc_dom_signal is not None and btc_dom_signal.score > 0,
        "Alt Risk-On Probability Up",
        "Macro liquidity is positive, capital flow is positive, and BTC dominance is falling/broadening.",
    )
    add_rule(
        "BTC-Led Risk-On",
        capital.status == "Positive" and etf_signal is not None and etf_signal.score > 0 and (btc_dom_signal is None or btc_dom_signal.score <= 0),
        "BTC-Led Risk-On",
        "ETF/capital flow is supportive while BTC dominance is not yet broadening into alt risk.",
    )
    add_rule(
        "On-Chain Leads Macro",
        onchain.status == "Bullish" and macro.status in {"Neutral", "Negative"},
        "Early Cycle Watch",
        "On-chain has improved before macro liquidity has fully confirmed.",
    )
    add_rule(
        "Liquidity Trap",
        macro.status == "Positive" and capital.status != "Positive",
        "Liquidity Not Yet Entering Crypto",
        "Macro backdrop is supportive but capital-flow confirmation is missing.",
    )
    add_rule(
        "Risk-Off Confirmation",
        macro.status == "Negative" and capital.status == "Negative" and onchain.status == "Bearish",
        "Risk-Off Confirmation",
        "Macro, capital flow, and on-chain structure are all negative.",
    )
    add_rule(
        "Narrative Weakness Drag",
        narrative.status == "Weak" and capital.status != "Positive",
        "Narrative Drag",
        "Sector/narrative rotation is weak and capital flow is not offsetting it.",
    )

    for layer_name, layer in layer_map.items():
        if layer.max_score == 0:
            add_rule(
                f"{layer_name} Missing Data",
                True,
                "Data Gap",
                f"{layer_name} has no live signals available, so regime confidence is lower.",
            )

    return rules


def match_historical_regime(macro: LayerResult, capital: LayerResult, onchain: LayerResult, narrative: LayerResult) -> dict[str, Any]:
    current = {
        "macro": macro.ratio,
        "capital": capital.ratio,
        "onchain": onchain.ratio,
        "narrative": narrative.ratio,
    }
    matches = []
    for record in HISTORICAL_REGIME_DATABASE:
        distance = sum((current[key] - float(record[key])) ** 2 for key in current) ** 0.5
        similarity = max(0.0, 1.0 - distance / 2.0)
        matches.append(
            {
                "period": record["period"],
                "regime": record["regime"],
                "similarity": round(similarity, 3),
                "notes": record["notes"],
                "scores": {key: record[key] for key in current},
                "btc_forward_30d_median": record.get("btc_forward_30d_median"),
                "btc_forward_30d_max": record.get("btc_forward_30d_max"),
                "btc_forward_30d_min": record.get("btc_forward_30d_min"),
            }
        )
    matches.sort(key=lambda row: row["similarity"], reverse=True)
    return {"current_vector": current, "best_match": matches[0], "matches": matches}


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _available_layers(layers: list[LayerResult]) -> list[LayerResult]:
    return [layer for layer in layers if layer.max_score > 0]


def compute_indicator_alignment(layers: list[LayerResult]) -> float:
    signals = [signal.score for layer in layers for signal in layer.signals if signal.score != 0]
    if not signals:
        return 0.0
    net_direction = 1 if sum(signals) >= 0 else -1
    aligned = sum(1 for score in signals if score * net_direction > 0)
    return aligned / len(signals)


def compute_volatility_regime_score(macro: LayerResult) -> tuple[float, str]:
    vix_signal = next((signal for signal in macro.signals if signal.indicator == "VIX"), None)
    vix_value = _safe_float(vix_signal.value if vix_signal else None)
    if vix_value is None:
        return 0.50, "Unknown volatility regime"
    if vix_value <= 18:
        return 1.00, "Calm volatility"
    if vix_value <= 25:
        return 0.65, "Normal volatility"
    return 0.30, "Elevated volatility"


def build_confidence_score(macro: LayerResult, capital: LayerResult, onchain: LayerResult, narrative: LayerResult, historical_match: dict[str, Any]) -> dict[str, Any]:
    layers = [macro, capital, onchain, narrative]
    alignment = compute_indicator_alignment(layers)
    similarity = float(historical_match.get("best_match", {}).get("similarity", 0.0))
    volatility_score, volatility_label = compute_volatility_regime_score(macro)
    data_coverage = len(_available_layers(layers)) / len(layers)
    score = (alignment * 0.35) + (similarity * 0.30) + (volatility_score * 0.20) + (data_coverage * 0.15)
    label = "High" if score >= 0.70 else "Medium" if score >= 0.45 else "Low"
    return {
        "label": label,
        "score_pct": round(score * 100, 1),
        "components": {
            "indicator_alignment_pct": round(alignment * 100, 1),
            "historical_similarity_pct": round(similarity * 100, 1),
            "volatility_regime": volatility_label,
            "volatility_score_pct": round(volatility_score * 100, 1),
            "data_coverage_pct": round(data_coverage * 100, 1),
        },
    }


def build_transition_probabilities(macro: LayerResult, capital: LayerResult, onchain: LayerResult, narrative: LayerResult) -> dict[str, int]:
    macro_ratio = macro.ratio
    capital_ratio = capital.ratio
    onchain_ratio = onchain.ratio
    narrative_ratio = narrative.ratio
    btc_dom_signal = next((signal for signal in capital.signals if signal.indicator == "BTC Dominance"), None)
    btc_dom_broadening = 1 if btc_dom_signal and btc_dom_signal.score > 0 else -1 if btc_dom_signal and btc_dom_signal.score < 0 else 0

    probabilities = {
        "Liquidity Expansion": 35 + macro_ratio * 35 + capital_ratio * 12,
        "BTC-Led Risk-On": 35 + capital_ratio * 30 + onchain_ratio * 18 - narrative_ratio * 8 - btc_dom_broadening * 6,
        "Early Rotation": 32 + onchain_ratio * 28 + capital_ratio * 12 - macro_ratio * 6,
        "Narrative Expansion": 40 + macro_ratio * 18 + capital_ratio * 22 + narrative_ratio * 25 + btc_dom_broadening * 10,
        "Exhaustion": 28 - narrative_ratio * 22 - onchain_ratio * 12 + max(0, capital_ratio) * 8,
        "Transition": 55 - abs(macro_ratio + capital_ratio + onchain_ratio + narrative_ratio) * 12,
        "Compression": 30 - macro_ratio * 24 - capital_ratio * 24 - onchain_ratio * 18 - narrative_ratio * 10,
    }
    return {name: int(round(_clamp(value, 0, 95))) for name, value in probabilities.items()}


def resolve_market_ontology(macro: LayerResult, capital: LayerResult, onchain: LayerResult, narrative: LayerResult, aggregate: float) -> str:
    etf_signal = next((signal for signal in capital.signals if signal.indicator == "BTC ETF Flow Proxy"), None)
    if macro.status == "Positive" and capital.status == "Positive" and narrative.status == "Strong":
        return "Narrative Expansion"
    if macro.status == "Positive" and capital.status == "Positive":
        return "Liquidity Expansion"
    if capital.status == "Positive" and etf_signal is not None and etf_signal.score > 0:
        return "BTC-Led Risk-On"
    if onchain.status == "Bullish" and capital.status != "Positive":
        return "Early Rotation"
    if aggregate <= -0.35:
        return "Compression"
    if narrative.status == "Weak" and aggregate < 0.15:
        return "Exhaustion"
    return "Transition"


def build_historical_outcome_explorer(historical_match: dict[str, Any]) -> dict[str, Any]:
    best = historical_match.get("best_match", {})
    if not best:
        return {}
    return {
        "period": best.get("period", "n/a"),
        "regime": best.get("regime", "n/a"),
        "similarity_pct": round(float(best.get("similarity", 0)) * 100, 1),
        "btc_forward_30d_median": best.get("btc_forward_30d_median"),
        "btc_forward_30d_max": best.get("btc_forward_30d_max"),
        "btc_forward_30d_min": best.get("btc_forward_30d_min"),
        "notes": best.get("notes", ""),
    }


def build_narrative_rotation_heatmap(narrative: LayerResult) -> list[dict[str, Any]]:
    sector_rotation = narrative.metadata.get("sector_rotation", {})
    rows = sector_rotation.get("narratives", [])
    if rows:
        return rows
    return [
        {"narrative": "AI", "change_pct": None, "strength": "Unavailable"},
        {"narrative": "RWA", "change_pct": None, "strength": "Unavailable"},
        {"narrative": "Meme", "change_pct": None, "strength": "Unavailable"},
        {"narrative": "DeFi", "change_pct": None, "strength": "Unavailable"},
    ]


def build_market_regime_engine(macro: LayerResult, capital: LayerResult, onchain: LayerResult, narrative: LayerResult) -> dict[str, Any]:
    layers = [macro, capital, onchain, narrative]
    available = _available_layers(layers)
    aggregate = sum(layer.ratio for layer in available) / len(available) if available else 0.0
    if aggregate >= 0.35:
        regime = "Risk-On"
    elif aggregate >= 0.12:
        regime = "Early Risk-On"
    elif aggregate <= -0.35:
        regime = "Risk-Off"
    elif aggregate <= -0.12:
        regime = "Early Risk-Off"
    else:
        regime = "Neutral / Transition"
    historical_match = match_historical_regime(macro, capital, onchain, narrative)
    market_ontology = resolve_market_ontology(macro, capital, onchain, narrative, aggregate)
    return {
        "macro": macro,
        "capital": capital,
        "onchain": onchain,
        "narrative": narrative,
        "aggregate_score": round(aggregate, 3),
        "market_regime": regime,
        "market_ontology": market_ontology,
        "contextual_rules": build_contextual_regime_rules(macro, capital, onchain, narrative),
        "historical_match": historical_match,
        "confidence": build_confidence_score(macro, capital, onchain, narrative, historical_match),
        "transition_probabilities": build_transition_probabilities(macro, capital, onchain, narrative),
        "historical_outcome": build_historical_outcome_explorer(historical_match),
        "narrative_heatmap": build_narrative_rotation_heatmap(narrative),
    }


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


def print_source_health(source: str, context: dict[str, Any]) -> None:
    print("  Source health:")
    latest_date = context.get("onchain_latest_date", "unknown")
    freshness = context.get("onchain_freshness_status", "unknown")
    age_days = context.get("onchain_row_age_days", "unknown")
    execution_ended = context.get("dune_execution_ended_at")
    execution_text = f", executed={execution_ended}" if execution_ended else ""
    print(f"    Dune/on-chain: source={source}, latest_row={latest_date}, age={age_days}d, status={freshness}{execution_text}")

    yfinance_date = context.get("yfinance_latest_date")
    yfinance_status = context.get("yfinance_status", "not configured")
    print(f"    yfinance: {yfinance_status}" + (f", latest={yfinance_date}" if yfinance_date else ""))

    if context.get("etherscan_safe_gas_gwei") is not None or context.get("etherscan_propose_gas_gwei") is not None:
        gas = context.get("etherscan_safe_gas_gwei") or context.get("etherscan_propose_gas_gwei")
        print(f"    Etherscan: live gas oracle, gas={gas} gwei")
    elif context.get("fetch_etherscan_context_error"):
        print(f"    Etherscan: error={context['fetch_etherscan_context_error']}")
    else:
        print("    Etherscan: unavailable")

    if context.get("cardanoscan_latest_block"):
        print("    Cardanoscan: live latest block")
    elif context.get("fetch_cardanoscan_context_error"):
        print(f"    Cardanoscan: error={context['fetch_cardanoscan_context_error']}")
    else:
        print("    Cardanoscan: unavailable")

    if context.get("solscan_latest_block"):
        print("    Solscan: live latest block")
    elif context.get("fetch_solscan_context_error"):
        print(f"    Solscan: error={context['fetch_solscan_context_error']}")
    else:
        print("    Solscan: unavailable")

    if context.get("onchain_freshness_warning"):
        print(f"    WARNING: {context['onchain_freshness_warning']}")
    if context.get("dune_direct_refresh_warning"):
        print(f"    WARNING: {context['dune_direct_refresh_warning']}")
    if context.get("dune_refresh_warning"):
        print(f"    WARNING: {context['dune_refresh_warning']}")


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


def _heatmap_color(kind: str, value: Any) -> str:
    if kind == "state":
        return STATE_COLORS.get(str(value), "#667085")
    numeric = _safe_float(value)
    if numeric is None or pd.isna(numeric):
        return "#e2e8f0"
    if kind == "ratio":
        if numeric >= 0.35:
            return "#047857"
        if numeric >= 0.15:
            return "#84cc16"
        if numeric <= -0.35:
            return "#b91c1c"
        if numeric <= -0.15:
            return "#f59e0b"
        return "#94a3b8"
    if numeric > 0:
        return "#16a34a"
    if numeric < 0:
        return "#dc2626"
    return "#94a3b8"


def _heatmap_value_label(kind: str, value: Any) -> str:
    if kind == "state":
        return str(value)
    numeric = _safe_float(value)
    if numeric is None or pd.isna(numeric):
        return "n/a"
    if kind == "ratio":
        return f"{numeric:+.2f}"
    return SCORE_LABEL.get(int(numeric), str(value))


def render_market_heatmap(summary: pd.DataFrame, days: int = 60) -> str:
    heatmap = summary.tail(days).reset_index(drop=True)
    if heatmap.empty:
        return '<div class="heatmap-empty">No market history available.</div>'

    candidate_rows = [
        ("Market State", "market_state", "state"),
        ("Total Score", "score_ratio", "ratio"),
        ("SOPR", "sopr_7d_ma_score", "score"),
        ("RHODL", "rhodl_ratio_7d_ma_score", "score"),
        ("Exchange Flow", "exchange_flow_7d_sum_score", "score"),
        ("LTH/STH", "lth_sth_rotation_score", "score"),
        ("Miner", "miner_behaviour_puell_7d_score", "score"),
        ("BTC Momentum", "yfinance_btc_momentum_score", "score"),
    ]
    rows = [(label, column, kind) for label, column, kind in candidate_rows if column in heatmap.columns]
    date_labels = "".join(
        f'<div class="heatmap-date" data-day="{idx}">{_html_cell(pd.to_datetime(row["date"]).strftime("%m-%d"))}</div>'
        for idx, row in heatmap.iterrows()
    )
    row_html: list[str] = []
    for label, column, kind in rows:
        cells = []
        row_slug = label.lower().replace("/", "-").replace(" ", "-")
        for idx, row in heatmap.iterrows():
            value = row.get(column)
            date = row.get("date", "")
            color = _heatmap_color(kind, value)
            value_label = _heatmap_value_label(kind, value)
            cells.append(
                f'<div class="heatmap-cell" style="background:{color}" '
                f'data-day="{idx}" data-row="{_html_cell(row_slug)}" data-label="{_html_cell(label)}" '
                f'data-date="{_html_cell(date)}" data-value="{_html_cell(value_label)}" '
                f'title="{_html_cell(date)} | {_html_cell(label)}: {_html_cell(value_label)}"></div>'
            )
        row_html.append(
            f'<div class="heatmap-label" data-row-label="{_html_cell(row_slug)}">{_html_cell(label)}</div>'
            f'<div class="heatmap-cells" data-row-cells="{_html_cell(row_slug)}">{"".join(cells)}</div>'
        )

    return f"""
    <div class="interaction-bar">
      <label>Timeframe
        <select id="heatmapTimeframe">
          <option value="60" selected>60D</option>
          <option value="30">30D</option>
          <option value="14">14D</option>
        </select>
      </label>
      <label>Signal
        <select id="heatmapSignal">
          <option value="all" selected>All signals</option>
          {''.join(f'<option value="{_html_cell(label.lower().replace("/", "-").replace(" ", "-"))}">{_html_cell(label)}</option>' for label, _, _ in rows)}
        </select>
      </label>
      <button type="button" id="resetHeatmap">Reset</button>
    </div>
    <div class="heatmap-legend">
      <span><i class="legend-bull"></i>Bullish</span>
      <span><i class="legend-neutral"></i>Neutral</span>
      <span><i class="legend-caution"></i>Caution</span>
      <span><i class="legend-bear"></i>Bearish</span>
    </div>
    <div class="heatmap">
      <div class="heatmap-label heatmap-label-muted">Date</div>
      <div class="heatmap-cells">{date_labels}</div>
      {''.join(row_html)}
    </div>
    <div class="drilldown-panel" id="heatmapDrilldown">
      <span>Drill-down</span>
      <strong>Click any heatmap cell</strong>
      <p>Date, signal, and vote will appear here.</p>
    </div>
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


def render_layer_rows(layer: LayerResult) -> str:
    if not layer.signals:
        return '<tr><td colspan="6">No live signals available for this layer.</td></tr>'
    return "\n".join(
        f"""
        <tr>
            <td>{_html_cell(signal.source)}</td>
            <td>{_html_cell(signal.indicator)}</td>
            <td>{_html_cell(signal.value)}</td>
            <td class="score score-{signal.score}">{SCORE_LABEL.get(signal.score, signal.score)}</td>
            <td>{_html_cell(signal.signal)}</td>
            <td class="rule-cell"><code>{_html_cell(signal.detail)}</code></td>
        </tr>
        """.strip()
        for signal in layer.signals
    )


def render_regime_snapshot(regime: dict[str, Any]) -> str:
    rows = [
        ("Macro Liquidity", regime["macro"].status),
        ("Capital Flow", regime["capital"].status),
        ("On-chain", regime["onchain"].status),
        ("Narrative Strength", regime["narrative"].status),
        ("Market Regime", regime["market_regime"]),
    ]
    return "\n".join(f"<tr><td>{_html_cell(label)}</td><td>{_html_cell(status)}</td></tr>" for label, status in rows)


def render_contextual_rules(regime: dict[str, Any]) -> str:
    rules = regime.get("contextual_rules", [])
    if not rules:
        return '<tr><td colspan="4">No contextual rules available.</td></tr>'
    return "\n".join(
        f"""
        <tr>
            <td>{_html_cell(rule["rule"])}</td>
            <td>{'Yes' if rule["triggered"] else 'No'}</td>
            <td>{_html_cell(rule["result"])}</td>
            <td>{_html_cell(rule["explanation"])}</td>
        </tr>
        """.strip()
        for rule in rules
    )


def render_historical_regime_rows(regime: dict[str, Any]) -> str:
    matches = regime.get("historical_match", {}).get("matches", [])
    if not matches:
        return '<tr><td colspan="4">No historical matches available.</td></tr>'
    return "\n".join(
        f"""
        <tr>
            <td>{_html_cell(match["period"])}</td>
            <td>{_html_cell(match["regime"])}</td>
            <td>{float(match["similarity"]) * 100:.1f}%</td>
            <td>{_html_cell(match["notes"])}</td>
        </tr>
        """.strip()
        for match in matches
    )


def render_confidence_scoring(regime: dict[str, Any]) -> str:
    confidence = regime.get("confidence", {})
    components = confidence.get("components", {})
    if not confidence:
        return '<tr><td colspan="2">No confidence score available.</td></tr>'
    rows = [
        ("Confidence", f"{confidence.get('label', 'n/a')} ({confidence.get('score_pct', 0)}%)"),
        ("Indicator Alignment", f"{components.get('indicator_alignment_pct', 0)}%"),
        ("Historical Similarity", f"{components.get('historical_similarity_pct', 0)}%"),
        ("Volatility Regime", components.get("volatility_regime", "n/a")),
        ("Data Coverage", f"{components.get('data_coverage_pct', 0)}%"),
    ]
    return "\n".join(f"<tr><td>{_html_cell(label)}</td><td>{_html_cell(value)}</td></tr>" for label, value in rows)


def render_transition_probabilities(regime: dict[str, Any]) -> str:
    probabilities = regime.get("transition_probabilities", {})
    if not probabilities:
        return '<tr><td colspan="2">No transition probabilities available.</td></tr>'
    ordered = sorted(probabilities.items(), key=lambda item: item[1], reverse=True)
    return "\n".join(f"<tr><td>{_html_cell(name)}</td><td>{int(value)}%</td></tr>" for name, value in ordered)


def render_probability_bars(regime: dict[str, Any]) -> str:
    probabilities = regime.get("transition_probabilities", {})
    if not probabilities:
        return '<div class="empty-note">No probability distribution available.</div>'
    ordered = sorted(probabilities.items(), key=lambda item: item[1], reverse=True)
    return "\n".join(
        f"""
        <div class="prob-row">
          <div class="prob-label">{_html_cell(name)}</div>
          <div class="prob-track"><span data-prob-bar="{_html_cell(name)}" data-base="{int(value)}" style="width:{int(value)}%"></span></div>
          <div class="prob-value" data-prob-value="{_html_cell(name)}">{int(value)}%</div>
        </div>
        """.strip()
        for name, value in ordered
    )


def render_key_drivers(regime: dict[str, Any]) -> str:
    drivers: list[tuple[str, str, int]] = []
    for layer_key in ("macro", "capital", "onchain", "narrative"):
        layer = regime[layer_key]
        ranked = sorted(layer.signals, key=lambda signal: abs(signal.score), reverse=True)
        for signal in ranked:
            if signal.score != 0:
                drivers.append((layer.layer, f"{signal.indicator}: {signal.signal}", signal.score))
                break
    if not drivers:
        drivers.append(("Read", "Signals are mixed; no single directional driver dominates.", 0))
    return "\n".join(
        f"""
        <div class="driver driver-{score}">
          <span>{_html_cell(layer)}</span>
          <strong>{_html_cell(text)}</strong>
        </div>
        """.strip()
        for layer, text, score in drivers[:4]
    )


def render_regime_wheel(regime: dict[str, Any]) -> str:
    labels = [
        "Liquidity Expansion",
        "BTC-Led Risk-On",
        "Early Rotation",
        "Narrative Expansion",
        "Exhaustion",
        "Transition",
        "Compression",
    ]
    active = regime.get("market_ontology", "Transition")
    items = []
    for idx, label in enumerate(labels):
        angle = -90 + (360 / len(labels)) * idx
        x = 140 + 104 * math.cos(math.radians(angle))
        y = 140 + 104 * math.sin(math.radians(angle))
        klass = "wheel-node active" if label == active else "wheel-node"
        items.append(
            f'<div class="{klass}" style="left:{x:.1f}px; top:{y:.1f}px;" title="{_html_cell(label)}">{_html_cell(label)}</div>'
        )
    return f"""
    <div class="regime-wheel">
      <div class="wheel-ring"></div>
      <div class="wheel-core">
        <span>Current</span>
        <strong>{_html_cell(active)}</strong>
      </div>
      {''.join(items)}
    </div>
    """


def render_historical_outcome_explorer(regime: dict[str, Any]) -> str:
    outcome = regime.get("historical_outcome", {})
    if not outcome:
        return '<tr><td colspan="2">No historical outcome match available.</td></tr>'
    rows = [
        ("When Regime Resembled", f"{outcome.get('period', 'n/a')} ({outcome.get('similarity_pct', 0)}% similarity)"),
        ("Matched Regime", outcome.get("regime", "n/a")),
        ("BTC Forward 30D Median", _signed_pct(outcome.get("btc_forward_30d_median"))),
        ("BTC Forward 30D Max", _signed_pct(outcome.get("btc_forward_30d_max"))),
        ("BTC Forward 30D Min", _signed_pct(outcome.get("btc_forward_30d_min"))),
        ("Read", outcome.get("notes", "")),
    ]
    return "\n".join(f"<tr><td>{_html_cell(label)}</td><td>{_html_cell(value)}</td></tr>" for label, value in rows)


def render_narrative_heatmap(regime: dict[str, Any]) -> str:
    rows = regime.get("narrative_heatmap", [])
    if not rows:
        return '<tr><td colspan="3">No narrative rotation data available.</td></tr>'
    return "\n".join(
        f"""
        <tr>
            <td>{_html_cell(row.get("narrative", "n/a"))}</td>
            <td>{_html_cell(_signed_pct(row.get("change_pct")))}</td>
            <td>{_html_cell(row.get("strength", "n/a"))}</td>
        </tr>
        """.strip()
        for row in rows
    )


def layer_to_dict(layer: LayerResult) -> dict[str, Any]:
    return {
        "layer": layer.layer,
        "score": layer.score,
        "max_score": layer.max_score,
        "ratio": layer.ratio,
        "status": layer.status,
        "signals": [signal.__dict__ for signal in layer.signals],
        "metadata": layer.metadata,
    }


def regime_to_dict(regime: dict[str, Any]) -> dict[str, Any]:
    return {
        "market_regime": regime["market_regime"],
        "market_ontology": regime.get("market_ontology"),
        "aggregate_score": regime["aggregate_score"],
        "contextual_rules": regime.get("contextual_rules", []),
        "historical_match": regime.get("historical_match", {}),
        "confidence": regime.get("confidence", {}),
        "transition_probabilities": regime.get("transition_probabilities", {}),
        "historical_outcome": regime.get("historical_outcome", {}),
        "narrative_heatmap": regime.get("narrative_heatmap", []),
        "layers": {
            "macro": layer_to_dict(regime["macro"]),
            "capital": layer_to_dict(regime["capital"]),
            "onchain": layer_to_dict(regime["onchain"]),
            "narrative": layer_to_dict(regime["narrative"]),
        },
    }


def _html_cell(value: Any) -> str:
    return html.escape("" if value is None else str(value))


def _signed_pct(value: Any) -> str:
    numeric = _safe_float(value)
    return "n/a" if numeric is None else f"{numeric:+.1f}%"


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

    if context.get("onchain_freshness_warning"):
        notes.append(str(context["onchain_freshness_warning"]))
    if context.get("dune_direct_refresh_warning"):
        notes.append(str(context["dune_direct_refresh_warning"]))
    if context.get("dune_refresh_warning"):
        notes.append(str(context["dune_refresh_warning"]))

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


def render_methodology_rows() -> str:
    rows = [
        (
            "Layer scoring",
            "Each live indicator votes -1, 0, or +1. Layer score is normalized by available max score so missing sources reduce coverage but do not fabricate a signal.",
        ),
        (
            "Historical similarity",
            "The current macro, capital-flow, on-chain, and narrative layer vector is compared with historical regime templates using Euclidean distance.",
        ),
        (
            "Regime probabilities",
            "Transition probabilities are heuristic scenario scores derived from layer ratios, BTC dominance behavior, narrative strength, and on-chain confirmation.",
        ),
        (
            "Confidence scoring",
            "Confidence blends indicator alignment, historical similarity, volatility regime, and data coverage. Higher agreement and fuller data raise confidence.",
        ),
        (
            "Proxy assumptions",
            "Public-data proxies are used when institutional-grade datasets are unavailable, including Dune-derived SOPR/RHODL, ETF signed dollar volume, and exchange reserve pressure.",
        ),
    ]
    return "\n".join(f"<tr><td>{_html_cell(label)}</td><td>{_html_cell(detail)}</td></tr>" for label, detail in rows)


def render_limitation_items(source: str) -> str:
    items = [
        "This system is designed for probabilistic market-state analysis, not deterministic price prediction.",
        "Several metrics are proxy-based because exact vendor-grade cost-basis, ETF flow, and exchange-reserve datasets are not always public.",
        "Live status depends on API availability, saved Dune query schema, and rate limits from third-party providers.",
        "Historical regime matching is a reference framework, not a claim that future returns will repeat past regimes.",
        "When the report says source=demo, outputs are for local testing only. Use source=dune for the live on-chain layer.",
    ]
    if source == "dune":
        items[-1] = "This run is using live Dune rows for the on-chain layer; supplemental sources vote only when their API responses are usable."
    return "\n".join(f"<li>{_html_cell(item)}</li>" for item in items)


def write_html_report(
    latest: ClassificationResult,
    summary: pd.DataFrame,
    output_path: Path,
    source: str,
    context: dict[str, Any],
    backtest: dict[str, Any],
    regime: dict[str, Any],
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
    market_heatmap = render_market_heatmap(summary)
    spark_rows = render_indicator_spark_table(summary)
    distribution_rows, forward_rows = render_backtest_tables(backtest)
    quality_notes = "\n".join(f"<li>{_html_cell(note)}</li>" for note in build_data_quality_notes(source, context))
    regime_rows = render_regime_snapshot(regime)
    contextual_rule_rows = render_contextual_rules(regime)
    historical_regime_rows = render_historical_regime_rows(regime)
    confidence_rows = render_confidence_scoring(regime)
    transition_probability_rows = render_transition_probabilities(regime)
    probability_bars = render_probability_bars(regime)
    key_driver_cards = render_key_drivers(regime)
    regime_wheel = render_regime_wheel(regime)
    historical_outcome_rows = render_historical_outcome_explorer(regime)
    narrative_heatmap_rows = render_narrative_heatmap(regime)
    methodology_rows = render_methodology_rows()
    limitation_items = render_limitation_items(source)
    best_match = regime.get("historical_match", {}).get("best_match", {})
    best_match_text = (
        "n/a"
        if not best_match
        else f"{best_match.get('period')} ({float(best_match.get('similarity', 0)) * 100:.1f}% similarity)"
    )
    top_transition = max(regime.get("transition_probabilities", {"Transition": 0}).items(), key=lambda item: item[1])
    macro_rows = render_layer_rows(regime["macro"])
    capital_rows = render_layer_rows(regime["capital"])
    narrative_rows = render_layer_rows(regime["narrative"])

    html = f"""<!doctype html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>On-Chain Market Report</title>
  <style>
    :root {{
      color-scheme: light;
      --ink: #111827;
      --muted: #64748b;
      --line: #d9e2ec;
      --bg: #f3f6f9;
      --panel: #ffffff;
      --panel-soft: #f8fafc;
      --brand: #0f172a;
      --accent: #2563eb;
      --bull: #047857;
      --bear: #b91c1c;
      --neutral: #475569;
      --shadow: 0 18px 42px rgba(15, 23, 42, 0.08);
    }}
    body {{
      margin: 0;
      font-family: Inter, ui-sans-serif, system-ui, -apple-system, BlinkMacSystemFont, "Segoe UI", sans-serif;
      background: var(--bg);
      color: var(--ink);
      line-height: 1.5;
    }}
    main {{
      width: min(1180px, calc(100% - 32px));
      margin: 24px auto 56px;
    }}
    header {{
      display: grid;
      grid-template-columns: 1fr auto;
      gap: 24px;
      align-items: center;
      padding: 26px 28px;
      background: var(--brand);
      color: white;
      border: 1px solid #1e293b;
      box-shadow: var(--shadow);
    }}
    h1, h2 {{ margin: 0; letter-spacing: 0; }}
    h1 {{ font-size: clamp(28px, 4vw, 44px); line-height: 1.05; }}
    h2 {{
      font-size: 13px;
      margin-top: 30px;
      margin-bottom: 12px;
      color: var(--muted);
      font-weight: 800;
      letter-spacing: .08em;
      text-transform: uppercase;
    }}
    .eyebrow {{
      color: #93c5fd;
      font-size: 12px;
      font-weight: 800;
      letter-spacing: .14em;
      text-transform: uppercase;
      margin-bottom: 8px;
    }}
    .meta {{ color: #cbd5e1; margin-top: 10px; }}
    .state {{
      text-align: right;
      min-width: 260px;
    }}
    .state strong {{
      display: block;
      font-size: 28px;
      line-height: 1.1;
    }}
    .score-pill {{
      display: inline-block;
      margin-top: 10px;
      padding: 8px 12px;
      border: 1px solid rgba(255, 255, 255, 0.18);
      background: rgba(255, 255, 255, 0.08);
      font-weight: 700;
      color: #e2e8f0;
    }}
    .report-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 18px;
      align-items: start;
    }}
    .executive-grid {{
      display: grid;
      grid-template-columns: minmax(300px, 0.85fr) minmax(420px, 1.15fr);
      gap: 18px;
      align-items: stretch;
      margin-top: 18px;
    }}
    .section {{
      background: var(--panel);
      border: 1px solid var(--line);
      box-shadow: 0 8px 24px rgba(15, 23, 42, 0.04);
      padding: 18px;
      margin-top: 18px;
    }}
    .section-title {{
      margin: 0 0 12px;
      font-size: 13px;
      color: var(--muted);
      font-weight: 800;
      letter-spacing: .08em;
      text-transform: uppercase;
    }}
    .section-wide {{
      grid-column: 1 / -1;
    }}
    .hero-panel {{
      background: #0f172a;
      color: #e2e8f0;
      border-color: #1e293b;
      min-height: 320px;
    }}
    .hero-panel .section-title {{ color: #93c5fd; }}
    .hero-regime {{
      font-size: clamp(34px, 5vw, 56px);
      line-height: 0.95;
      letter-spacing: 0;
      color: #ffffff;
      margin: 14px 0;
    }}
    .hero-copy {{
      color: #cbd5e1;
      max-width: 46rem;
      margin: 0 0 18px;
    }}
    .hero-kpis {{
      display: grid;
      grid-template-columns: repeat(3, minmax(0, 1fr));
      gap: 10px;
      margin-top: 18px;
    }}
    .hero-kpi {{
      border: 1px solid rgba(255, 255, 255, 0.14);
      background: rgba(255, 255, 255, 0.06);
      padding: 12px;
    }}
    .hero-kpi span {{
      display: block;
      color: #94a3b8;
      font-size: 12px;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: .05em;
    }}
    .hero-kpi b {{
      display: block;
      color: #ffffff;
      font-size: 18px;
      margin-top: 4px;
    }}
    .driver-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 10px;
      margin-top: 16px;
    }}
    .driver {{
      border: 1px solid rgba(255, 255, 255, 0.14);
      background: rgba(255, 255, 255, 0.06);
      padding: 12px;
    }}
    .driver span {{
      display: block;
      color: #94a3b8;
      font-size: 12px;
      font-weight: 800;
      margin-bottom: 6px;
    }}
    .driver strong {{
      color: #e2e8f0;
      font-size: 13px;
    }}
    .driver-1 {{ border-left: 3px solid #22c55e; }}
    .driver-0 {{ border-left: 3px solid #94a3b8; }}
    .driver--1 {{ border-left: 3px solid #ef4444; }}
    .focus-panel {{
      display: grid;
      grid-template-columns: 300px 1fr;
      gap: 18px;
      align-items: center;
    }}
    .regime-wheel {{
      position: relative;
      width: 280px;
      height: 280px;
      margin: 0 auto;
    }}
    .wheel-ring {{
      position: absolute;
      inset: 32px;
      border: 1px solid #dbe3ec;
      border-radius: 50%;
      background: radial-gradient(circle, #ffffff 0 45%, #f8fafc 46% 100%);
    }}
    .wheel-core {{
      position: absolute;
      left: 82px;
      top: 88px;
      width: 116px;
      height: 104px;
      border-radius: 50%;
      display: grid;
      place-items: center;
      text-align: center;
      background: #0f172a;
      color: white;
      padding: 8px;
      box-shadow: 0 16px 34px rgba(15, 23, 42, 0.18);
    }}
    .wheel-core span {{
      display: block;
      color: #93c5fd;
      font-size: 10px;
      font-weight: 800;
      letter-spacing: .08em;
      text-transform: uppercase;
    }}
    .wheel-core strong {{
      display: block;
      font-size: 13px;
      line-height: 1.1;
      margin-top: 4px;
    }}
    .wheel-node {{
      position: absolute;
      width: 104px;
      min-height: 28px;
      transform: translate(-50%, -50%);
      display: grid;
      place-items: center;
      text-align: center;
      padding: 5px 6px;
      background: #eef2f6;
      border: 1px solid #d9e2ec;
      color: #475569;
      font-size: 10px;
      font-weight: 800;
      line-height: 1.1;
      text-transform: uppercase;
    }}
    .wheel-node.active {{
      background: #2563eb;
      border-color: #1d4ed8;
      color: white;
      box-shadow: 0 10px 24px rgba(37, 99, 235, 0.28);
    }}
    .prob-row {{
      display: grid;
      grid-template-columns: 152px 1fr 44px;
      gap: 10px;
      align-items: center;
      margin: 9px 0;
    }}
    .prob-label {{
      color: var(--ink);
      font-size: 13px;
      font-weight: 800;
    }}
    .prob-track {{
      height: 10px;
      background: #e2e8f0;
      overflow: hidden;
    }}
    .prob-track span {{
      display: block;
      height: 100%;
      background: #2563eb;
    }}
    .prob-value {{
      color: var(--muted);
      font-size: 12px;
      font-weight: 800;
      text-align: right;
    }}
    .quality {{
      margin: 18px 0 0;
      padding: 16px 18px;
      background: #fff7ed;
      border: 1px solid var(--line);
      border-left: 4px solid #f59e0b;
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
      padding: 11px 12px;
      border-bottom: 1px solid var(--line);
      text-align: left;
      vertical-align: top;
      font-size: 14px;
      overflow-wrap: anywhere;
    }}
    th {{
      color: var(--muted);
      font-weight: 800;
      background: var(--panel-soft);
      font-size: 12px;
      letter-spacing: .04em;
      text-transform: uppercase;
    }}
    tr:last-child td {{ border-bottom: 0; }}
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
    .heatmap {{
      display: grid;
      grid-template-columns: 150px 1fr;
      gap: 8px 12px;
      align-items: center;
    }}
    .heatmap-cells {{
      display: grid;
      grid-template-columns: repeat(60, minmax(8px, 1fr));
      gap: 3px;
      min-width: 540px;
    }}
    .heatmap-cell {{
      height: 18px;
      border: 1px solid rgba(15, 23, 42, 0.08);
    }}
    .heatmap-date {{
      color: var(--muted);
      font-size: 9px;
      text-align: center;
      writing-mode: vertical-rl;
      transform: rotate(180deg);
      height: 46px;
      white-space: nowrap;
    }}
    .heatmap-label {{
      color: var(--ink);
      font-weight: 800;
      font-size: 13px;
    }}
    .heatmap-label-muted {{
      color: var(--muted);
    }}
    .heatmap-legend {{
      display: flex;
      flex-wrap: wrap;
      gap: 14px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 700;
      margin-bottom: 14px;
    }}
    .heatmap-legend span {{
      display: inline-flex;
      align-items: center;
      gap: 6px;
    }}
    .heatmap-legend i {{
      width: 12px;
      height: 12px;
      display: inline-block;
      border: 1px solid rgba(15, 23, 42, 0.1);
    }}
    .legend-bull {{ background: #16a34a; }}
    .legend-neutral {{ background: #94a3b8; }}
    .legend-caution {{ background: #f59e0b; }}
    .legend-bear {{ background: #dc2626; }}
    .heatmap-empty {{
      padding: 16px;
      color: var(--muted);
      background: var(--panel-soft);
      border: 1px solid var(--line);
    }}
    .interaction-bar {{
      display: flex;
      flex-wrap: wrap;
      gap: 10px;
      align-items: center;
      justify-content: space-between;
      margin-bottom: 14px;
      padding: 12px;
      background: var(--panel-soft);
      border: 1px solid var(--line);
    }}
    .interaction-bar label {{
      display: inline-flex;
      align-items: center;
      gap: 8px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: .04em;
    }}
    select, button, input[type="range"] {{
      accent-color: var(--accent);
    }}
    select, button {{
      border: 1px solid var(--line);
      background: white;
      color: var(--ink);
      padding: 7px 10px;
      font: inherit;
      font-size: 13px;
    }}
    button {{
      cursor: pointer;
      font-weight: 800;
    }}
    .heatmap-cell {{
      cursor: crosshair;
    }}
    .heatmap-cell.is-selected {{
      outline: 2px solid #0f172a;
      outline-offset: 1px;
    }}
    .is-hidden {{
      display: none !important;
    }}
    .drilldown-panel {{
      display: grid;
      grid-template-columns: 110px 1fr;
      gap: 6px 14px;
      margin-top: 14px;
      padding: 14px;
      background: var(--panel-soft);
      border: 1px solid var(--line);
    }}
    .drilldown-panel span {{
      color: var(--muted);
      font-size: 12px;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: .05em;
      grid-row: span 2;
    }}
    .drilldown-panel strong {{
      color: var(--ink);
    }}
    .drilldown-panel p {{
      margin: 0;
      color: var(--muted);
    }}
    .scenario-grid {{
      display: grid;
      grid-template-columns: repeat(2, minmax(0, 1fr));
      gap: 12px;
      margin-top: 16px;
    }}
    .scenario-control {{
      padding: 12px;
      border: 1px solid var(--line);
      background: var(--panel-soft);
    }}
    .scenario-control label {{
      display: flex;
      justify-content: space-between;
      gap: 12px;
      color: var(--muted);
      font-size: 12px;
      font-weight: 800;
      text-transform: uppercase;
      letter-spacing: .04em;
      margin-bottom: 8px;
    }}
    .scenario-control input {{
      width: 100%;
    }}
    .scenario-note {{
      margin-top: 14px;
      padding: 12px;
      border-left: 3px solid var(--accent);
      background: #eff6ff;
      color: #1e3a8a;
      font-size: 13px;
      font-weight: 700;
    }}
    .compact-table td:first-child {{
      color: var(--muted);
      font-weight: 700;
      width: 34%;
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
      padding: 14px;
      box-shadow: 0 6px 18px rgba(15, 23, 42, 0.04);
    }}
    .kpi b {{
      display: block;
      font-size: 20px;
      margin-top: 4px;
    }}
    .regime-table td:first-child {{
      color: var(--muted);
      font-size: 15px;
      font-weight: 700;
    }}
    .regime-table td:last-child {{
      font-size: 18px;
      font-weight: 800;
      text-align: right;
    }}
    .callout {{
      background: var(--panel-soft);
      border: 1px solid var(--line);
      padding: 12px 14px;
      margin-top: 12px;
      color: var(--muted);
    }}
    .disclaimer {{
      background: #f8fafc;
      border-left: 4px solid #64748b;
    }}
    details.section {{
      padding: 0;
    }}
    details.section summary {{
      cursor: pointer;
      list-style: none;
      padding: 16px 18px;
      color: var(--muted);
      font-size: 13px;
      font-weight: 800;
      letter-spacing: .08em;
      text-transform: uppercase;
    }}
    details.section summary::-webkit-details-marker {{ display: none; }}
    details.section summary::after {{
      content: "Open";
      float: right;
      color: var(--accent);
      font-size: 12px;
      letter-spacing: 0;
      text-transform: none;
    }}
    details.section[open] summary::after {{ content: "Close"; }}
    .details-body {{
      padding: 0 18px 18px;
    }}
    .empty-note {{
      color: var(--muted);
      padding: 12px;
      background: var(--panel-soft);
      border: 1px solid var(--line);
    }}
    ul {{
      margin: 0;
      padding-left: 20px;
      color: var(--muted);
    }}
    @media (max-width: 760px) {{
      header {{ grid-template-columns: 1fr; }}
      .executive-grid {{ grid-template-columns: 1fr; }}
      .focus-panel {{ grid-template-columns: 1fr; }}
      .report-grid {{ grid-template-columns: 1fr; }}
      .hero-kpis, .driver-grid {{ grid-template-columns: 1fr; }}
      .scenario-grid {{ grid-template-columns: 1fr; }}
      .section-wide {{ grid-column: auto; }}
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
        <div class="eyebrow">Crypto Market Intelligence</div>
        <h1>Market Regime Command Center</h1>
        <div class="meta">Generated {datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")} | data source: {source}</div>
      </div>
      <div class="state">
        <strong>{_html_cell(regime.get("market_ontology", regime.get("market_regime", "Transition")))}</strong>
        <div class="score-pill">Confidence {regime.get("confidence", {}).get("label", "n/a")} | On-chain {latest.total_score:+d}/{latest.max_score}</div>
      </div>
    </header>

    <section class="section hero-panel">
      <div class="section-title">Executive Read</div>
      <div class="hero-regime">{_html_cell(regime.get("market_ontology", "Transition"))}</div>
      <p class="hero-copy">The framework classifies the current setup through macro liquidity, capital flow, on-chain structure, and narrative rotation. Heavy rule detail is kept in the appendix below.</p>
      <div class="hero-kpis">
        <div class="hero-kpi"><span>Regime</span><b>{_html_cell(regime.get("market_regime", "n/a"))}</b></div>
        <div class="hero-kpi"><span>Confidence</span><b>{regime.get("confidence", {}).get("label", "n/a")} ({regime.get("confidence", {}).get("score_pct", 0)}%)</b></div>
        <div class="hero-kpi"><span>Top Transition</span><b>{_html_cell(top_transition[0])} {int(top_transition[1])}%</b></div>
      </div>
      <div class="driver-grid">{key_driver_cards}</div>
    </section>

    <div class="executive-grid">
      <section class="section focus-panel">
        {regime_wheel}
        <div>
          <div class="section-title">Market Ontology</div>
          <p class="hero-copy" style="color: var(--muted);">A custom seven-state taxonomy: Liquidity Expansion, BTC-Led Risk-On, Early Rotation, Narrative Expansion, Exhaustion, Transition, and Compression.</p>
          <div class="callout"><b>Closest historical analog:</b> {_html_cell(best_match_text)}</div>
        </div>
      </section>

      <section class="section">
        <div class="section-title">Regime Probability Distribution</div>
        {probability_bars}
        <div class="scenario-grid" id="scenarioEngine">
          <div class="scenario-control">
            <label>BTC dominance <span id="btcDominanceValue">54%</span></label>
            <input type="range" min="45" max="65" value="54" id="btcDominanceSlider">
          </div>
          <div class="scenario-control">
            <label>Stablecoin growth <span id="stableGrowthValue">0%</span></label>
            <input type="range" min="-5" max="8" value="0" id="stableGrowthSlider">
          </div>
          <div class="scenario-control">
            <label>Macro impulse <span id="macroImpulseValue">0</span></label>
            <input type="range" min="-2" max="2" value="0" id="macroImpulseSlider">
          </div>
          <div class="scenario-control">
            <label>Narrative strength <span id="narrativeImpulseValue">0</span></label>
            <input type="range" min="-2" max="2" value="0" id="narrativeImpulseSlider">
          </div>
        </div>
        <div class="scenario-note" id="scenarioRead">Scenario engine: adjust assumptions to model regime-transition pressure.</div>
      </section>
    </div>

    <section class="section section-wide">
    <h2>Signal Heatmap</h2>
    <div class="chart-wrap">
      {market_heatmap}
    </div>
    </section>

    <section class="section">
    <h2>Narrative Rotation Heatmap</h2>
    <table>
      <thead>
        <tr><th>Narrative</th><th>24H Change</th><th>Strength</th></tr>
      </thead>
      <tbody>{narrative_heatmap_rows}</tbody>
    </table>
    </section>

    <section class="section">
    <h2>Historical Outcome Explorer</h2>
    <table class="compact-table">
      <thead>
        <tr><th>Outcome</th><th>Read</th></tr>
      </thead>
      <tbody>{historical_outcome_rows}</tbody>
    </table>
    </section>

    <details class="section section-wide">
      <summary>Layer Details</summary>
      <div class="details-body">
        <h2>Layer 1: Macro Liquidity</h2>
        <table>
          <thead><tr><th>Source</th><th>Indicator</th><th>Value</th><th>Score</th><th>Signal</th><th>Rule</th></tr></thead>
          <tbody>{macro_rows}</tbody>
        </table>
        <h2>Layer 2: Capital Flow</h2>
        <table>
          <thead><tr><th>Source</th><th>Indicator</th><th>Value</th><th>Score</th><th>Signal</th><th>Rule</th></tr></thead>
          <tbody>{capital_rows}</tbody>
        </table>
        <h2>Narrative Strength</h2>
        <table>
          <thead><tr><th>Source</th><th>Indicator</th><th>Value</th><th>Score</th><th>Signal</th><th>Rule</th></tr></thead>
          <tbody>{narrative_rows}</tbody>
        </table>
        <h2>Layer 3: On-Chain Indicator Read</h2>
        <table>
          <thead><tr><th>Source</th><th>Indicator</th><th>Value</th><th>Score</th><th>Signal</th><th>Rule</th></tr></thead>
          <tbody>{latest_rows}</tbody>
        </table>
      </div>
    </details>

    <details class="section section-wide">
      <summary>Rules, Confidence, And Methodology</summary>
      <div class="details-body">
        <h2>Confidence Scoring</h2>
        <table class="compact-table">
          <thead><tr><th>Component</th><th>Read</th></tr></thead>
          <tbody>{confidence_rows}</tbody>
        </table>
        <h2>Contextual Regime Logic</h2>
        <table>
          <thead><tr><th>Rule</th><th>Triggered</th><th>Result</th><th>Explanation</th></tr></thead>
          <tbody>{contextual_rule_rows}</tbody>
        </table>
        <h2>Methodology</h2>
        <table class="compact-table">
          <thead><tr><th>Area</th><th>Method</th></tr></thead>
          <tbody>{methodology_rows}</tbody>
        </table>
      </div>
    </details>

    <details class="section section-wide">
      <summary>Historical Database And Backtest</summary>
      <div class="details-body">
        <h2>Historical Regime Database</h2>
        <table>
          <thead><tr><th>Period</th><th>Regime</th><th>Similarity</th><th>Notes</th></tr></thead>
          <tbody>{historical_regime_rows}</tbody>
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
          <thead><tr><th>Market State</th><th>Count</th></tr></thead>
          <tbody>{distribution_rows}</tbody>
        </table>
        <h2>Forward Returns By State</h2>
        <table>
          <thead><tr><th>Market State</th><th>Samples</th><th>Mean 7D Return</th><th>Median 7D Return</th></tr></thead>
          <tbody>{forward_rows}</tbody>
        </table>
        <h2>Recent Classifications</h2>
        <table>
          <thead><tr><th>Date</th><th>Score</th><th>Market State</th><th>Conviction</th></tr></thead>
          <tbody>{recent_rows}</tbody>
        </table>
      </div>
    </details>

    <section class="section section-wide">
    <h2>Indicator Sparklines</h2>
    <table>
      <thead>
        <tr><th>Indicator</th><th>Latest</th><th>30-Point Sparkline</th></tr>
      </thead>
      <tbody>{spark_rows}</tbody>
    </table>
    </section>

    <section class="section disclaimer section-wide">
    <h2>Disclaimer / Limitations</h2>
    <ul>{limitation_items}</ul>
    </section>

    <details class="section section-wide">
      <summary>Data Quality And Source Status</summary>
      <div class="details-body">
        <h2>Data Quality Notes</h2>
        <ul>{quality_notes}</ul>
        <h2>Cross-Chain Source Status</h2>
        <ul>{context_items}</ul>
      </div>
    </details>
  </main>
  <script>
    const clamp = (value, min, max) => Math.max(min, Math.min(max, value));

    function updateHeatmap() {{
      const timeframe = Number(document.getElementById("heatmapTimeframe")?.value || 60);
      const signal = document.getElementById("heatmapSignal")?.value || "all";
      const allCells = Array.from(document.querySelectorAll(".heatmap-cell, .heatmap-date"));
      const dayValues = allCells.map((node) => Number(node.dataset.day)).filter((value) => !Number.isNaN(value));
      const maxDay = Math.max(...dayValues, 0);
      const minVisibleDay = Math.max(0, maxDay - timeframe + 1);

      document.querySelectorAll(".heatmap-date, .heatmap-cell").forEach((node) => {{
        const day = Number(node.dataset.day);
        node.classList.toggle("is-hidden", day < minVisibleDay);
      }});
      document.querySelectorAll("[data-row-label], [data-row-cells]").forEach((node) => {{
        const row = node.dataset.rowLabel || node.dataset.rowCells;
        node.classList.toggle("is-hidden", signal !== "all" && row !== signal);
      }});
    }}

    function bindHeatmap() {{
      document.getElementById("heatmapTimeframe")?.addEventListener("change", updateHeatmap);
      document.getElementById("heatmapSignal")?.addEventListener("change", updateHeatmap);
      document.getElementById("resetHeatmap")?.addEventListener("click", () => {{
        document.getElementById("heatmapTimeframe").value = "60";
        document.getElementById("heatmapSignal").value = "all";
        updateHeatmap();
      }});
      document.querySelectorAll(".heatmap-cell").forEach((cell) => {{
        cell.addEventListener("click", () => {{
          document.querySelectorAll(".heatmap-cell.is-selected").forEach((node) => node.classList.remove("is-selected"));
          cell.classList.add("is-selected");
          const panel = document.getElementById("heatmapDrilldown");
          if (panel) {{
            panel.innerHTML = `<span>Drill-down</span><strong>${{cell.dataset.date}} | ${{cell.dataset.label}}</strong><p>Signal read: ${{cell.dataset.value}}</p>`;
          }}
        }});
      }});
      updateHeatmap();
    }}

    function bindScenarioEngine() {{
      const sliders = {{
        btc: document.getElementById("btcDominanceSlider"),
        stable: document.getElementById("stableGrowthSlider"),
        macro: document.getElementById("macroImpulseSlider"),
        narrative: document.getElementById("narrativeImpulseSlider"),
      }};
      if (!sliders.btc || !sliders.stable || !sliders.macro || !sliders.narrative) return;

      const update = () => {{
        const btc = Number(sliders.btc.value);
        const stable = Number(sliders.stable.value);
        const macro = Number(sliders.macro.value);
        const narrative = Number(sliders.narrative.value);
        document.getElementById("btcDominanceValue").textContent = `${{btc}}%`;
        document.getElementById("stableGrowthValue").textContent = `${{stable}}%`;
        document.getElementById("macroImpulseValue").textContent = macro;
        document.getElementById("narrativeImpulseValue").textContent = narrative;

        const adjustments = {{
          "Liquidity Expansion": stable * 2.0 + macro * 7,
          "BTC-Led Risk-On": (btc >= 55 ? 8 : -4) + macro * 3,
          "Early Rotation": (btc < 54 ? 8 : -3) + stable * 1.2,
          "Narrative Expansion": (btc < 54 ? 12 : -6) + stable * 1.8 + narrative * 9,
          "Exhaustion": (btc > 58 ? 7 : 0) - stable * 1.2 - narrative * 7,
          "Transition": -Math.abs(macro + narrative) * 4,
          "Compression": -stable * 1.8 - macro * 8 - narrative * 3,
        }};

        let leader = ["Transition", 0];
        document.querySelectorAll("[data-prob-bar]").forEach((bar) => {{
          const name = bar.dataset.probBar;
          const base = Number(bar.dataset.base || 0);
          const value = Math.round(clamp(base + (adjustments[name] || 0), 0, 95));
          bar.style.width = `${{value}}%`;
          const label = document.querySelector(`[data-prob-value="${{CSS.escape(name)}}"]`);
          if (label) label.textContent = `${{value}}%`;
          if (value > leader[1]) leader = [name, value];
        }});

        const note = document.getElementById("scenarioRead");
        if (note) {{
          note.textContent = `Scenario read: ${{leader[0]}} becomes the strongest transition at ${{leader[1]}}%.`;
        }}
      }};

      Object.values(sliders).forEach((slider) => slider.addEventListener("input", update));
      update();
    }}

    bindHeatmap();
    bindScenarioEngine();
  </script>
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
    regime: dict[str, Any],
) -> dict[str, Path]:
    output_dir.mkdir(parents=True, exist_ok=True)
    summary_path = output_dir / "onchain_summary.csv"
    html_path = output_dir / "onchain_report.html"
    context_path = output_dir / "source_context.json"
    backtest_path = output_dir / "backtest_report.json"
    regime_path = output_dir / "regime_report.json"

    summary.to_csv(summary_path, index=False)
    write_html_report(latest, summary, html_path, source=source, context=context, backtest=backtest, regime=regime)
    context_path.write_text(json.dumps(context, indent=2, default=str), encoding="utf-8")
    backtest_path.write_text(json.dumps(backtest, indent=2, default=str), encoding="utf-8")
    regime_path.write_text(json.dumps(regime_to_dict(regime), indent=2, default=str), encoding="utf-8")
    return {
        "summary_csv": summary_path,
        "html_report": html_path,
        "source_context": context_path,
        "backtest_report": backtest_path,
        "regime_report": regime_path,
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
    for attr_key, context_key in (
        ("dune_execution_id", "dune_execution_id"),
        ("dune_execution_ended_at", "dune_execution_ended_at"),
        ("dune_execution_submitted_at", "dune_execution_submitted_at"),
        ("dune_max_age_hours", "dune_max_age_hours"),
        ("dune_refresh_status", "dune_refresh_status"),
        ("dune_direct_refresh_warning", "dune_direct_refresh_warning"),
        ("dune_refresh_warning", "dune_refresh_warning"),
        ("onchain_latest_date", "onchain_latest_date"),
        ("onchain_row_age_days", "onchain_row_age_days"),
        ("onchain_freshness_status", "onchain_freshness_status"),
        ("onchain_freshness_warning", "onchain_freshness_warning"),
    ):
        if raw.attrs.get(attr_key) is not None:
            cross_chain_context[context_key] = raw.attrs[attr_key]
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
    macro_layer = build_macro_liquidity_layer(prefer_live=prefer_live)
    capital_layer = build_capital_flow_layer(summary, prefer_live=prefer_live)
    onchain_layer = build_onchain_layer_result(latest)
    narrative_layer = build_narrative_strength_layer(capital_layer)
    regime = build_market_regime_engine(macro_layer, capital_layer, onchain_layer, narrative_layer)
    cross_chain_context["market_regime"] = regime["market_regime"]
    cross_chain_context["market_regime_aggregate_score"] = regime["aggregate_score"]
    artifacts = export_artifacts(
        summary,
        latest,
        output_dir,
        source=source,
        context=cross_chain_context,
        backtest=backtest,
        regime=regime,
    )

    if verbose:
        print_report(latest, source=source)
        print()
        print_source_health(source, cross_chain_context)
        print()
        print("  Market regime engine:")
        for label, layer in (
            ("Macro Liquidity", macro_layer),
            ("Capital Flow", capital_layer),
            ("On-chain", onchain_layer),
            ("Narrative Strength", narrative_layer),
        ):
            print(f"    {label:<20} {layer.status:<22} ({layer.score:+d}/+{layer.max_score})")
        print(f"    {'Market Regime':<20} {regime['market_regime']:<22} ({regime['aggregate_score']:+.2f})")
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
            dune_max_age_hours=env_config.dune_max_age_hours,
            dune_refresh_on_run=env_config.dune_refresh_on_run,
            dune_refresh_timeout_seconds=env_config.dune_refresh_timeout_seconds,
            dune_poll_seconds=env_config.dune_poll_seconds,
            stale_after_days=env_config.stale_after_days,
            calibration=env_config.calibration,
        )

    run_pipeline(
        config=env_config,
        window=args.window,
        prefer_live=not args.demo,
        output_dir=args.output_dir,
        verbose=True,
    )
