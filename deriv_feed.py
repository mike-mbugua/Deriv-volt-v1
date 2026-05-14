"""
deriv_feed.py
═════════════════════════════════════════════════════════════════════
Deriv market-data client.  Drop-in alongside data_feed.py.

Two backends are supported — use whichever you have access to:

  1. DerivWebSocketClient  (recommended)
     ─────────────────────────────────────
     Uses Deriv's official JSON WebSocket API (wss://ws.binaryws.com).
     Requires only a Deriv API token (read-only scope is enough).
     Granularities map to Deriv's `ticks_history` `granularity` field.

  2. DerivMT5Client  (alternative)
     ────────────────────────────────
     Fetches candles via MetaTrader 5's Python library — exactly the
     same path the MT5Broker already uses for execution.
     Requires MT5 to be installed and a Deriv MT5 account logged in.

Both expose the same interface as OandaClient so DataFeed / PaperTrader
can call either without modification.

DerivDataFeed
─────────────
High-level caching layer — identical API to DataFeed in data_feed.py.
Wraps either DerivWebSocketClient or DerivMT5Client.

Supported synthetic instruments (Deriv names):
  Volatility 10  Index  →  R_10
  Volatility 25  Index  →  R_25
  Volatility 50  Index  →  R_50
  Volatility 75  Index  →  R_75
  Volatility 100 Index  →  R_100
  Crash  300 Index      →  CRASH300N
  Crash  500 Index      →  CRASH500
  Crash  1000 Index     →  CRASH1000
  Boom   300 Index      →  BOOM300N
  Boom   500 Index      →  BOOM500
  Boom   1000 Index     →  BOOM1000
  Step   Index          →  STPIDX
  Jump   10  Index      →  JD10
  Jump   25  Index      →  JD25
  Jump   50  Index      →  JD50
  Jump   75  Index      →  JD75
  Jump   100 Index      →  JD100

Forex pairs on Deriv (same format as OANDA but routed through Deriv):
  frxGBPUSD, frxEURUSD, frxUSDJPY, frxXAUUSD, frxUSDCAD, etc.
  These are mapped automatically by the symbol helpers below.

Setup
─────
Add to your .env:
  DERIV_API_TOKEN=your_deriv_api_token_here

  # If using MT5 backend:
  MT5_LOGIN=12345678
  MT5_PASSWORD=your_mt5_password
  MT5_SERVER=Deriv-Demo          # or Deriv-Server for live
  MT5_PATH=                      # optional: path to terminal64.exe

How to get a Deriv API token:
  1. Log in to app.deriv.com
  2. Go to Account Settings → API Token
  3. Create a token with "Read" scope — that is enough for market data.
     Add "Trade" scope if you also want to place orders via WebSocket.
"""

import json
import logging
import os
import time
import threading
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

import pandas as pd
import numpy as np
import requests

logger = logging.getLogger(__name__)

# ================================================================== #
#  Symbol maps                                                        #
# ================================================================== #

# Deriv WebSocket API uses these symbol strings in ticks_history calls.
# Synthetics use their short code; forex uses the "frx" prefix.
DERIV_SYMBOL_MAP: Dict[str, str] = {
    # ── Synthetic indices ─────────────────────────────────────────── #
    "R_10":      "R_10",
    "R_25":      "R_25",
    "R_50":      "R_50",
    "R_75":      "R_75",
    "R_100":     "R_100",
    "CRASH300N": "CRASH300N",
    "CRASH500":  "CRASH500",
    "CRASH1000": "CRASH1000",
    "BOOM300N":  "BOOM300N",
    "BOOM500":   "BOOM500",
    "BOOM1000":  "BOOM1000",
    "STPIDX":    "STPIDX",
    "JD10":      "JD10",
    "JD25":      "JD25",
    "JD50":      "JD50",
    "JD75":      "JD75",
    "JD100":     "JD100",
    # ── Deriv forex (frx prefix) ──────────────────────────────────── #
    "frxGBPUSD": "frxGBPUSD",
    "frxEURUSD": "frxEURUSD",
    "frxUSDJPY": "frxUSDJPY",
    "frxXAUUSD": "frxXAUUSD",
    "frxUSDCAD": "frxUSDCAD",
    "frxUSDCHF": "frxUSDCHF",
    "frxGBPJPY": "frxGBPJPY",
    "frxEURJPY": "frxEURJPY",
    "frxAUDUSD": "frxAUDUSD",
    "frxAUDJPY": "frxAUDJPY",
    "frxNZDJPY": "frxNZDJPY",
    "frxAUDNZD": "frxAUDNZD",
    "frxGBPAUD": "frxGBPAUD",
    "frxEURAUD": "frxEURAUD",
    "frxXAGUSD": "frxXAGUSD",
    # ── OANDA-format aliases → Deriv frx ─────────────────────────── #
    "GBP_USD": "frxGBPUSD",
    "EUR_USD": "frxEURUSD",
    "USD_JPY": "frxUSDJPY",
    "XAU_USD": "frxXAUUSD",
    "USD_CAD": "frxUSDCAD",
    "USD_CHF": "frxUSDCHF",
    "GBP_JPY": "frxGBPJPY",
    "EUR_JPY": "frxEURJPY",
    "AUD_USD": "frxAUDUSD",
    "AUD_JPY": "frxAUDJPY",
    "NZD_JPY": "frxNZDJPY",
    "AUD_NZD": "frxAUDNZD",
    "GBP_AUD": "frxGBPAUD",
    "EUR_AUD": "frxEURAUD",
    "XAG_USD": "frxXAGUSD",
}

# MT5 symbol names for Deriv (may vary by broker — edit if needed).
# Deriv MT5 typically uses the plain short code without "frx".
DERIV_MT5_SYMBOL_MAP: Dict[str, str] = {
    # Synthetics
    "R_10":      "Volatility 10 Index",
    "R_25":      "Volatility 25 Index",
    "R_50":      "Volatility 50 Index",
    "R_75":      "Volatility 75 Index",
    "R_100":     "Volatility 100 Index",
    "CRASH300N": "Crash 300 Index",
    "CRASH500":  "Crash 500 Index",
    "CRASH1000": "Crash 1000 Index",
    "BOOM300N":  "Boom 300 Index",
    "BOOM500":   "Boom 500 Index",
    "BOOM1000":  "Boom 1000 Index",
    "STPIDX":    "Step Index",
    "JD10":      "Jump 10 Index",
    "JD25":      "Jump 25 Index",
    "JD50":      "Jump 50 Index",
    "JD75":      "Jump 75 Index",
    "JD100":     "Jump 100 Index",
    # Forex via Deriv MT5
    "frxGBPUSD": "GBPUSD",
    "frxEURUSD": "EURUSD",
    "frxUSDJPY": "USDJPY",
    "frxXAUUSD": "XAUUSD",
    "frxUSDCAD": "USDCAD",
    "frxUSDCHF": "USDCHF",
    "frxGBPJPY": "GBPJPY",
    "frxEURJPY": "EURJPY",
    "frxAUDUSD": "AUDUSD",
    "frxAUDJPY": "AUDJPY",
    "frxNZDJPY": "NZDJPY",
    "frxAUDNZD": "AUDNZD",
    "frxGBPAUD": "GBPAUD",
    "frxEURAUD": "EURAUD",
    "frxXAGUSD": "XAGUSD",
    # OANDA-format pass-through
    "GBP_USD": "GBPUSD",
    "EUR_USD": "EURUSD",
    "USD_JPY": "USDJPY",
    "XAU_USD": "XAUUSD",
    "USD_CAD": "USDCAD",
    "USD_CHF": "USDCHF",
    "GBP_JPY": "GBPJPY",
    "EUR_JPY": "EURJPY",
    "AUD_USD": "AUDUSD",
    "AUD_JPY": "AUDJPY",
    "NZD_JPY": "NZDJPY",
    "AUD_NZD": "AUDNZD",
    "GBP_AUD": "GBPAUD",
    "EUR_AUD": "EURAUD",
    "XAG_USD": "XAGUSD",
    "R_10":   "Volatility 10 Index",
    "R_25":   "Volatility 25 Index",
    "R_50":   "Volatility 50 Index",
    "R_75":   "Volatility 75 Index",
    "R_100":  "Volatility 100 Index",
}

# Granularity → Deriv WebSocket seconds-per-candle
DERIV_GRANULARITY_MAP: Dict[str, int] = {
    "M1":  60,
    "M2":  120,
    "M5":  300,
    "M10": 600,
    "M15": 900,
    "M30": 1800,
    "H1":  3600,
    "H2":  7200,
    "H4":  14400,
    "H8":  28800,
    "H12": 43200,
    "D":   86400,
    "W":   604800,
}

# MT5 timeframe constants (accessed via mt5.TIMEFRAME_*)
MT5_TIMEFRAME_MAP: Dict[str, str] = {
    "M1":  "TIMEFRAME_M1",
    "M2":  "TIMEFRAME_M2",
    "M5":  "TIMEFRAME_M5",
    "M10": "TIMEFRAME_M10",
    "M15": "TIMEFRAME_M15",
    "M30": "TIMEFRAME_M30",
    "H1":  "TIMEFRAME_H1",
    "H2":  "TIMEFRAME_H2",
    "H4":  "TIMEFRAME_H4",
    "H8":  "TIMEFRAME_H8",
    "H12": "TIMEFRAME_H12",
    "D":   "TIMEFRAME_D1",
    "W":   "TIMEFRAME_W1",
}

# Max candles Deriv WebSocket returns per request
DERIV_MAX_CANDLES = 5000


# ================================================================== #
#  Helper: is this a synthetic index?                                 #
# ================================================================== #

SYNTHETIC_PREFIXES = ("R_", "CRASH", "BOOM", "STPIDX", "JD")

def is_synthetic(symbol: str) -> bool:
    """Return True if the symbol is a Deriv synthetic index."""
    s = symbol.upper()
    return any(s.startswith(p) for p in SYNTHETIC_PREFIXES)


# ================================================================== #
#  Backend 1 — Deriv WebSocket API                                   #
# ================================================================== #

class DerivWebSocketClient:
    """
    Fetches OHLCV candles from Deriv's WebSocket API using a
    synchronous HTTP-equivalent pattern (send request, wait for
    response with a matching req_id).

    Uses the websocket-client library (pip install websocket-client).

    Falls back to a simple requests-based approach when the WS library
    is not available — Deriv also exposes a REST-like endpoint for
    ticks_history.
    """

    WS_URL = "wss://ws.binaryws.com/websockets/v3?app_id=1089"
    # app_id=1089 is Deriv's public demo app ID — fine for read-only use.
    # For production, register your own app at https://app.deriv.com/account/api-token

    def __init__(self, api_token: str = None):
        self.api_token = api_token or os.getenv("DERIV_API_TOKEN", "")
        self._req_counter = 0
        self._lock = threading.Lock()

    def _next_req_id(self) -> int:
        with self._lock:
            self._req_counter += 1
            return self._req_counter

    # ── WebSocket send/receive ────────────────────────────────────── #

    def _ws_request(self, payload: dict, timeout: int = 30) -> dict:
        """
        Open a fresh WebSocket connection, send one request,
        wait for the matching response, then close.

        This is intentionally stateless — no persistent connection —
        which makes it robust for a polling bot that calls every N secs.
        """
        try:
            import websocket
        except ImportError:
            raise ImportError(
                "websocket-client is not installed.\n"
                "Run: pip install websocket-client\n"
                "Or use DerivMT5Client instead."
            )

        req_id         = self._next_req_id()
        payload["req_id"] = req_id
        if self.api_token:
            payload["passthrough"] = {"token": self.api_token}

        result     = {}
        error_flag = []

        def on_message(ws, message):
            data = json.loads(message)
            if data.get("req_id") == req_id:
                result.update(data)
                ws.close()

        def on_error(ws, error):
            error_flag.append(str(error))
            ws.close()

        ws = websocket.WebSocketApp(
            self.WS_URL,
            on_message=on_message,
            on_error=on_error,
        )
        thread = threading.Thread(
            target=lambda: ws.run_forever(ping_interval=20),
            daemon=True,
        )
        thread.start()
        thread.join(timeout=timeout)

        if error_flag:
            raise RuntimeError(f"Deriv WebSocket error: {error_flag[0]}")
        if not result:
            raise TimeoutError(f"Deriv WebSocket timed out after {timeout}s")
        if "error" in result:
            raise RuntimeError(
                f"Deriv API error: {result['error'].get('message', result['error'])}"
            )
        return result

    # ── Public interface (mirrors OandaClient) ────────────────────── #

    def get_candles(
        self,
        instrument:  str,
        granularity: str = "H1",
        count:       int = 500,
        from_time:   str = None,
        to_time:     str = None,
    ) -> pd.DataFrame:
        """
        Fetch OHLCV candles from Deriv.
        Returns DataFrame with columns: price, open, high, low, volume.
        """
        symbol = DERIV_SYMBOL_MAP.get(instrument, instrument)
        gran_secs = DERIV_GRANULARITY_MAP.get(granularity, 3600)

        # Deriv ticks_history request
        payload: dict = {
            "ticks_history": symbol,
            "style":         "candles",
            "granularity":   gran_secs,
            "adjust_start_time": 1,
        }

        if from_time and to_time:
            # Convert ISO strings to epoch if needed
            start = _to_epoch(from_time)
            end   = _to_epoch(to_time)
            payload["start"] = start
            payload["end"]   = end
        else:
            payload["count"] = min(count, DERIV_MAX_CANDLES)
            payload["end"]   = "latest"

        logger.debug(f"Deriv WS request: {symbol} {granularity} x{count}")
        data = self._ws_request(payload)

        candles = data.get("candles", [])
        if not candles:
            return pd.DataFrame(columns=["price", "open", "high", "low", "volume"])

        rows = []
        for c in candles:
            rows.append({
                "Date":   pd.to_datetime(c["epoch"], unit="s", utc=True).tz_localize(None),
                "price":  float(c["close"]),
                "open":   float(c["open"]),
                "high":   float(c["high"]),
                "low":    float(c["low"]),
                "volume": 0,   # Deriv synthetics have no volume
            })

        df = pd.DataFrame(rows).set_index("Date")
        df.sort_index(inplace=True)
        return df

    def get_long_history(
        self,
        instrument:  str,
        granularity: str = "H1",
        bars:        int = 2500,
    ) -> pd.DataFrame:
        """
        Fetch more than DERIV_MAX_CANDLES bars by paginating backwards.
        Deriv WS supports `start`/`end` epoch params for range queries.
        """
        gran_secs    = DERIV_GRANULARITY_MAP.get(granularity, 3600)
        chunk_size   = min(bars, DERIV_MAX_CANDLES)
        remaining    = bars
        all_frames   = []

        end_epoch = int(datetime.now(timezone.utc).timestamp())

        while remaining > 0:
            fetch_n    = min(remaining, chunk_size)
            start_epoch = end_epoch - fetch_n * gran_secs - gran_secs  # slight overlap

            symbol    = DERIV_SYMBOL_MAP.get(instrument, instrument)
            gran_secs_ = DERIV_GRANULARITY_MAP.get(granularity, 3600)

            payload = {
                "ticks_history":     symbol,
                "style":             "candles",
                "granularity":       gran_secs_,
                "start":             start_epoch,
                "end":               end_epoch,
                "adjust_start_time": 1,
            }

            try:
                data    = self._ws_request(payload)
                candles = data.get("candles", [])
            except Exception as e:
                logger.error(f"Deriv get_long_history chunk failed: {e}")
                break

            if not candles:
                break

            rows = []
            for c in candles:
                rows.append({
                    "Date":   pd.to_datetime(c["epoch"], unit="s", utc=True).tz_localize(None),
                    "price":  float(c["close"]),
                    "open":   float(c["open"]),
                    "high":   float(c["high"]),
                    "low":    float(c["low"]),
                    "volume": 0,
                })
            df = pd.DataFrame(rows).set_index("Date")
            df.sort_index(inplace=True)
            all_frames.append(df)

            remaining -= len(df)
            end_epoch  = candles[0]["epoch"] - 1   # move window back
            time.sleep(0.3)   # be kind to the API

            if len(df) < fetch_n // 2:
                break   # reached available history limit

        if not all_frames:
            return pd.DataFrame(columns=["price", "open", "high", "low", "volume"])

        combined = pd.concat(all_frames).drop_duplicates().sort_index()
        return combined.iloc[-bars:]

    def get_current_price(self, instrument: str) -> dict:
        """Return the latest tick price for an instrument."""
        symbol  = DERIV_SYMBOL_MAP.get(instrument, instrument)
        payload = {"ticks": symbol}
        data    = self._ws_request(payload)
        tick    = data.get("tick", {})
        mid     = float(tick.get("quote", 0))
        return {"bid": mid, "ask": mid, "mid": mid, "spread": 0.0}

    def get_account_summary(self) -> dict:
        """
        Fetch Deriv account balance via WebSocket.
        Requires a token with 'Read' scope.
        """
        if not self.api_token:
            logger.warning("No DERIV_API_TOKEN set — cannot fetch account summary.")
            return {}
        payload = {"authorize": self.api_token}
        auth    = self._ws_request(payload)
        bal     = auth.get("authorize", {})
        return {
            "balance":          float(bal.get("balance",   0)),
            "currency":         bal.get("currency",  "USD"),
            "nav":              float(bal.get("balance",   0)),
            "unrealized_pl":    0.0,
            "open_trade_count": 0,
            "margin_used":      0.0,
            "margin_available": float(bal.get("balance",   0)),
        }


# ================================================================== #
#  Backend 2 — Deriv via MetaTrader 5                                #
# ================================================================== #

class DerivMT5Client:
    """
    Fetches OHLCV candles directly from MetaTrader 5.
    Uses the same MT5 connection as MT5Broker in execution_broker.py.

    This is the preferred backend if you already have MT5 installed
    because:
      • No WebSocket library needed
      • Same authentication path as trade execution
      • MT5 data is already cached locally after first fetch

    Setup:
      pip install MetaTrader5
      Set MT5_LOGIN, MT5_PASSWORD, MT5_SERVER in .env
    """

    def __init__(
        self,
        login:    int = None,
        password: str = None,
        server:   str = None,
        path:     str = None,
    ):
        self.login    = login    or int(os.getenv("MT5_LOGIN",    0))
        self.password = password or os.getenv("MT5_PASSWORD",    "")
        self.server   = server   or os.getenv("MT5_SERVER",      "")
        self.path     = path     or os.getenv("MT5_PATH",        "")
        self._mt5     = None
        self._connected = False

    # ── Connection management ─────────────────────────────────────── #

    def connect(self) -> bool:
        try:
            import MetaTrader5 as mt5
            self._mt5 = mt5
        except ImportError:
            raise ImportError(
                "MetaTrader5 is not installed.\n"
                "Run: pip install MetaTrader5\n"
                "Or use DerivWebSocketClient instead."
            )

        init_kwargs = {}
        if self.path:
            init_kwargs["path"] = self.path

        if not self._mt5.initialize(**init_kwargs):
            logger.error(f"MT5 initialize() failed: {self._mt5.last_error()}")
            return False

        if self.login and self.password and self.server:
            ok = self._mt5.login(
                login    = self.login,
                password = self.password,
                server   = self.server,
            )
            if not ok:
                logger.error(f"MT5 login failed: {self._mt5.last_error()}")
                self._mt5.shutdown()
                return False

        self._connected = True
        logger.info("DerivMT5Client connected.")
        return True

    def disconnect(self):
        if self._mt5 and self._connected:
            self._mt5.shutdown()
            self._connected = False

    def _ensure_connected(self):
        if not self._connected:
            if not self.connect():
                raise RuntimeError("DerivMT5Client: MT5 not connected.")

    def _resolve_symbol(self, instrument: str) -> str:
        """Map instrument name to MT5 symbol string for Deriv."""
        sym = DERIV_MT5_SYMBOL_MAP.get(instrument, instrument.replace("_", ""))
        self._mt5.symbol_select(sym, True)   # make visible in MarketWatch
        return sym

    # ── Public interface (mirrors OandaClient) ────────────────────── #

    def get_candles(
        self,
        instrument:  str,
        granularity: str = "H1",
        count:       int = 500,
        from_time:   str = None,
        to_time:     str = None,
    ) -> pd.DataFrame:
        self._ensure_connected()
        symbol = self._resolve_symbol(instrument)
        tf     = getattr(self._mt5, MT5_TIMEFRAME_MAP.get(granularity, "TIMEFRAME_H1"))

        if from_time and to_time:
            start = datetime.fromisoformat(from_time.replace("Z", "+00:00"))
            rates = self._mt5.copy_rates_range(symbol, tf, start, datetime.now(timezone.utc))
        else:
            rates = self._mt5.copy_rates_from_pos(symbol, tf, 0, min(count, 50000))

        return _mt5_rates_to_df(rates)

    def get_long_history(
        self,
        instrument:  str,
        granularity: str = "H1",
        bars:        int = 2500,
    ) -> pd.DataFrame:
        """
        MT5 can return up to 50,000 bars in one call — no pagination needed
        for reasonable bar counts.
        """
        self._ensure_connected()
        symbol = self._resolve_symbol(instrument)
        tf     = getattr(self._mt5, MT5_TIMEFRAME_MAP.get(granularity, "TIMEFRAME_H1"))
        rates  = self._mt5.copy_rates_from_pos(symbol, tf, 0, min(bars, 50000))
        df     = _mt5_rates_to_df(rates)
        return df.iloc[-bars:] if len(df) > bars else df

    def get_current_price(self, instrument: str) -> dict:
        self._ensure_connected()
        symbol = self._resolve_symbol(instrument)
        tick   = self._mt5.symbol_info_tick(symbol)
        if tick is None:
            raise RuntimeError(f"No tick for {symbol} — check MarketWatch")
        bid = float(tick.bid)
        ask = float(tick.ask)
        mid = round((bid + ask) / 2, 5)
        return {"bid": bid, "ask": ask, "mid": mid, "spread": round(ask - bid, 5)}

    def get_account_summary(self) -> dict:
        self._ensure_connected()
        info = self._mt5.account_info()
        if info is None:
            return {}
        return {
            "balance":          float(info.balance),
            "currency":         info.currency,
            "nav":              float(info.equity),
            "unrealized_pl":    float(info.profit),
            "open_trade_count": len(self._mt5.positions_get() or []),
            "margin_used":      float(info.margin),
            "margin_available": float(info.margin_free),
        }

    def get_open_trades(self) -> list:
        self._ensure_connected()
        positions = self._mt5.positions_get() or []
        result    = []
        for p in positions:
            result.append({
                "instrument": p.symbol,
                "units":      p.volume * (1 if p.type == 0 else -1) * 100_000,
                "price":      float(p.price_open),
                "id":         str(p.ticket),
                "unrealizedPL": float(p.profit),
            })
        return result


# ================================================================== #
#  High-level DerivDataFeed (caching layer)                          #
# ================================================================== #

class DerivDataFeed:
    """
    Drop-in replacement for DataFeed in data_feed.py.
    Works with either DerivWebSocketClient or DerivMT5Client.

    Usage:
        client = DerivWebSocketClient()   # or DerivMT5Client()
        feed   = DerivDataFeed(client, cache_dir="data")
        df     = feed.get_history("R_75", "H1", 500)
        df2    = feed.get_history("GBP_USD", "H4", 500)   # OANDA alias works
    """

    _MAX_CACHE_AGE_HOURS: Dict[str, float] = {
        "W":   36.0,
        "D":   36.0,
        "H4":  12.0,
        "H1":   1.0,
        "M15":  0.5,
        "M5":   0.5,
        "M2":   0.1,
        "M1":   0.05,
    }

    def __init__(self, client, cache_dir: str = "data"):
        self.client    = client
        self.cache_dir = cache_dir
        os.makedirs(cache_dir, exist_ok=True)

    def _cache_path(self, instrument: str, granularity: str) -> str:
        safe = instrument.replace("/", "_").replace(" ", "_")
        return os.path.join(self.cache_dir, f"deriv_{safe}_{granularity}.csv")

    def _load_cache(self, instrument: str, granularity: str):
        path = self._cache_path(instrument, granularity)
        if not os.path.exists(path):
            return None
        try:
            df = pd.read_csv(path, parse_dates=["Date"], index_col="Date")
            df.sort_index(inplace=True)
            return df
        except Exception:
            return None

    def _save_cache(self, df: pd.DataFrame, instrument: str, granularity: str):
        df.to_csv(self._cache_path(instrument, granularity))

    def _cache_is_fresh(self, cached, granularity: str) -> bool:
        if cached is None or cached.empty:
            return False
        last_bar  = cached.index[-1]
        now_naive = datetime.now(timezone.utc).replace(tzinfo=None)
        age_hours = (now_naive - last_bar).total_seconds() / 3600
        max_age   = self._MAX_CACHE_AGE_HOURS.get(granularity, 1.0)
        # Synthetics trade 24/7 so we can be stricter about freshness
        return age_hours < max_age

    def get_history(
        self,
        instrument:    str,
        granularity:   str  = "H1",
        bars:          int  = 500,
        force_refresh: bool = False,
    ) -> pd.DataFrame:
        """
        Return OHLCV DataFrame for the given instrument and timeframe.
        Caches to CSV; re-fetches when stale.
        """
        if not force_refresh:
            cached = self._load_cache(instrument, granularity)
            if self._cache_is_fresh(cached, granularity):
                logger.info(
                    f"[Deriv] Using cached {instrument} {granularity} ({len(cached)} bars)"
                )
                return cached

        logger.info(
            f"[Deriv] Fetching {bars} bars: {instrument} {granularity}..."
        )
        try:
            df = self.client.get_long_history(instrument, granularity, bars)
        except Exception as e:
            logger.error(f"[Deriv] Fetch failed for {instrument} {granularity}: {e}")
            cached = self._load_cache(instrument, granularity)
            if cached is not None and not cached.empty:
                logger.warning(f"[Deriv] Falling back to stale cache: {instrument} {granularity}")
                return cached
            raise

        if df.empty:
            raise ValueError(f"[Deriv] No data returned for {instrument} {granularity}")

        self._save_cache(df, instrument, granularity)
        logger.info(f"[Deriv] Fetched {len(df)} bars: {instrument} {granularity}")
        return df

    def get_latest_candle(self, instrument: str, granularity: str = "H1") -> dict:
        df = self.client.get_candles(instrument, granularity, count=2)
        if df.empty:
            return {}
        row = df.iloc[-1]
        return {
            "time":   str(df.index[-1]),
            "price":  float(row["price"]),
            "open":   float(row["open"]),
            "high":   float(row["high"]),
            "low":    float(row["low"]),
            "volume": int(row.get("volume", 0)),
        }

    def refresh_all(
        self,
        instruments: List[str],
        granularity: str = "H1",
        bars:        int = 500,
    ) -> Dict[str, pd.DataFrame]:
        results = {}
        for inst in instruments:
            try:
                results[inst] = self.get_history(
                    inst, granularity, bars, force_refresh=True
                )
            except Exception as e:
                logger.error(f"[Deriv] Failed refresh {inst} {granularity}: {e}")
        return results


# ================================================================== #
#  Helpers                                                            #
# ================================================================== #

def _to_epoch(time_str: str) -> int:
    """Convert an ISO 8601 or RFC3339 timestamp string to Unix epoch int."""
    ts = time_str.replace("Z", "+00:00").replace(" ", "T")
    try:
        dt = datetime.fromisoformat(ts)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return int(dt.timestamp())
    except Exception:
        return int(time.time())


def _mt5_rates_to_df(rates) -> pd.DataFrame:
    """Convert MT5 copy_rates_* result (structured array) to DataFrame."""
    if rates is None or len(rates) == 0:
        return pd.DataFrame(columns=["price", "open", "high", "low", "volume"])
    df = pd.DataFrame(rates)
    df["Date"]  = pd.to_datetime(df["time"], unit="s", utc=True).dt.tz_localize(None)
    df          = df.rename(columns={"close": "price", "tick_volume": "volume"})
    df          = df[["Date", "price", "open", "high", "low", "volume"]]
    df          = df.set_index("Date").sort_index()
    return df