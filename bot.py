#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║  CRYPTO PUMP & DUMP SCANNER BOT V5                          ║
║  Quét USDT Perp: Binance, Bybit, BingX, KuCoin               ║
║  1D Trend/Squeeze + 1H Reversal Engine → Telegram           ║
╚══════════════════════════════════════════════════════════════╝
"""

import requests
import time
import json
import logging
import schedule
import threading
import html
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone
from dataclasses import dataclass, field
from typing import Optional, Any
from config import (
    COINGLASS_API_KEY, TELEGRAM_TOKEN, TELEGRAM_CHAT_ID,
    TOP_N, MIN_SCORE, SCAN_INTERVAL_HOURS,
    EXCLUDE_SYMBOLS, VOL_SPIKE_MIN, OI_DIV_MIN_PCT,
    FR_MAX_NORMAL, FR_SQUEEZE_THRESHOLD, LSR_MIN,
    LIQ_RATIO_MIN_GOOD, BOTTOM_PCT
)

# Có thể sửa nhanh tại đây
SCAN_EXCHANGES = ["Binance", "Bybit", "BingX", "KuCoin"]  # Quét cả 4 sàn
PER_EXCHANGE_TOP_N = False             # False = gộp cả 3 sàn rồi xếp điểm cao xuống thấp
TOP_N_FINAL = 3                         # Chỉ gửi 3 coin tiềm năng nhất
AUTO_SCAN_INTERVAL_SECONDS = 3600       # Scan tự động mỗi 1 giờ
MIN_VOL_RATIO_FILTER = 2.0              # Tăng 1.2→2.0: loại noise MOG/1INCH vol thấp (PUMP)
MIN_PRICE_CHANGE_FILTER = 5.0           # Loại coin tăng quá yếu nếu volume không đủ (PUMP)
MAX_LSR_HEALTHY = 2.30                  # L/S quá cao = crowded long, giảm điểm

# Ngưỡng riêng cho DUMP — thấp hơn pump vì dump không cần vol spike mạnh
MIN_DUMP_VOL_RATIO = 0.8               # Vol tối thiểu để xét dump (0.8 = không cần spike)
MIN_DUMP_PRICE_DROP = 3.0              # Drop tối thiểu 3% để lọt vào dump scan
MIN_DUMP_SCORE = 3.0                   # Ngưỡng điểm tối thiểu để lọt top dump

# ── 1H Reversal Engine ────────────────────────────────────────
ENABLE_1H_REVERSAL = True              # Bật/tắt scan 1H reversal
ENABLE_30MIN_SCAN = True               # Bật/tắt scan reversal mỗi 30 phút (xx:32 UTC)
DAILY_SCAN_HOUR   = 0                  # Giờ UTC chạy full scan 1D (0 = 00:02 UTC)
# Pump Reversal: coin pump mạnh 1D nhưng 1H đang đảo chiều xuống
PUMP_REV_1D_MIN_PUMP = 10.0           # 1D tăng tối thiểu 10% trước đó
PUMP_REV_1H_DROP = 3.0                # 1H hiện tại giảm ≥ 3%
PUMP_REV_1H_VOL_MULT = 1.5            # Vol 1H ≥ 1.5x MA10_1H
# Dump Reversal: coin dump mạnh 1D nhưng 1H đang bật ngược lên
DUMP_REV_1D_MIN_DUMP = 8.0            # 1D giảm tối thiểu 8% trước đó
DUMP_REV_1H_PUMP = 3.0                # 1H hiện tại tăng ≥ 3%
DUMP_REV_1H_VOL_MULT = 1.5            # Vol 1H ≥ 1.5x MA10_1H
INTRADAY_DUMP_MIN = 15.0              # Intraday dump (open→low) ≥ 15% trong nến ngày hiện tại
MIN_REVERSAL_SCORE = 3.0              # Điểm tối thiểu để lọt reversal list

# ── H2 Scan config ────────────────────────────────────────────
H2_MIN_CHG      = 7.0    # H2 tăng/giảm tối thiểu 7%
H2_MIN_VOL      = 1.3    # vol_ratio H2 tối thiểu (thấp hơn D vì H2 vol hay thấp)
H2_MIN_SCORE    = 4.0    # ngưỡng điểm (thấp hơn D=5.0)
H2_SCAN_HOURS   = 2      # quét mỗi 2H

# ── 1H Momentum Breakout ──────────────────────────────────────
# Signal độc lập với 1D — bắt nến 1H pump/dump mạnh có vol spike
# Ví dụ: MLNUSDT 07:00 UTC 14/5 — +10.37% vol 9.3x FR âm
H1_BREAKOUT_MIN_CHG     = 8.0    # 1H thay đổi tối thiểu (pump hoặc dump)
H1_BREAKOUT_MIN_VOL     = 5.0    # vol_ratio 1H tối thiểu (x MA10)
H1_BREAKOUT_MIN_SCORE   = 4.0    # Điểm tối thiểu để alert
H1_BREAKOUT_FR_BONUS    = -0.05  # FR âm ≤ ngưỡng này → bonus squeeze

# Engine mode
TREND_MIN_SCORE = 5.0                   # Ngưỡng nhận diện TREND coin kiểu IRYS
SQUEEZE_MIN_SCORE = 5.0                 # Ngưỡng nhận diện SQUEEZE coin kiểu COS
HYBRID_MIN_SCORE = 5.0                  # Cả trend + squeeze đều mạnh

# Tăng tốc scan
FAST_SCAN = True
MAX_WORKERS_BINANCE = 12   # Giữ — Binance weight-based, 12 là sweet spot
MAX_WORKERS_BYBIT  = 15   # Bybit limit 120 req/s, còn dư nhiều
MAX_WORKERS_BINGX  = 6    # BingX limit 10 req/s thực tế
MAX_WORKERS_KUCOIN = 5    # 5 workers + delay 80ms → ~10-12 req/s, safe với 30 req/min thực tế
KUCOIN_REQUEST_DELAY = 0.08  # 80ms delay giữa các request → ~12 req/s max

# Số workers tối đa cho parallel exchange scan (4 sàn chạy đồng thời)
MAX_WORKERS_EXCHANGES = 4  # Chạy Binance + Bybit + BingX + KuCoin song song
LOG_EVERY_N = 25           # Log tiến độ mỗi N coin thay vì in từng coin

# ── Logging ──────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scanner.log", encoding="utf-8")
    ]
)
log = logging.getLogger(__name__)

# ── API Base ─────────────────────────────────────────────────
COINGLASS_BASE = "https://open-api-v3.coinglass.com/api"
BINANCE_BASE = "https://fapi.binance.com"
BYBIT_BASE  = "https://api.bybit.com"
BINGX_BASE  = "https://open-api.bingx.com"
KUCOIN_BASE = "https://api-futures.kucoin.com"   # KuCoin Futures public API

# Rate limiter cho KuCoin — semaphore + delay để tránh 429
import threading as _threading
_kucoin_lock = _threading.Semaphore(MAX_WORKERS_KUCOIN)

CG_HEADERS = {
    "accept": "application/json",
    "CG-API-KEY": COINGLASS_API_KEY
}

_thread_local = threading.local()

def get_session() -> requests.Session:
    sess = getattr(_thread_local, "session", None)
    if sess is None:
        sess = requests.Session()
        _thread_local.session = sess
    return sess


# ══════════════════════════════════════════════════════════════
# DATA CLASSES
# ══════════════════════════════════════════════════════════════

@dataclass
class CoinData:
    symbol: str
    exchange: str = ""
    # Price & Volume
    open: float = 0
    high: float = 0
    low: float = 0
    close: float = 0
    volume: float = 0
    vol_ma10: float = 0
    price_change_pct: float = 0
    # Open Interest
    oi_current: float = 0
    oi_prev4: float = 0
    oi_change_pct: float = 0
    # Funding Rate
    funding_rate: float = 0
    # Long/Short Ratio
    lsr: float = 0
    # Liquidation
    liq_longs: float = 0
    liq_shorts: float = 0
    liq_ratio: float = 0
    # Long-term low
    low_20d: float = 0
    # 1D lookback: % change của 2 nến ngày trước (để bắt reversal sau pump/dump hôm qua)
    prev1d_change_pct: float = 0   # nến[-2]: hôm qua
    prev2d_change_pct: float = 0   # nến[-3]: hôm kia
    # Intraday dump: (open - low) / open — bắt case dump sâu trong ngày rồi bật lại
    intraday_dump_pct: float = 0   # % giá đã dump từ open xuống low trong nến ngày hiện tại
    # 1H Reversal data
    h1_open: float = 0
    h1_close: float = 0
    h1_high: float = 0
    h1_low: float = 0
    h1_volume: float = 0
    h1_vol_ma10: float = 0
    h1_price_change_pct: float = 0
    h1_available: bool = False         # True nếu lấy được 1H data
    # M30 data — xác nhận momentum cho reversal scan 30 phút
    m30_open: float = 0
    m30_close: float = 0
    m30_high: float = 0
    m30_low: float = 0
    m30_volume: float = 0
    m30_vol_ma10: float = 0
    m30_price_change_pct: float = 0
    m30_prev_change_pct: float = 0     # nến M30 trước đó (để xem trend M30)
    m30_available: bool = False

@dataclass
class ScoreResult:
    symbol: str
    exchange: str = ""
    total_score: float = 0
    score_cvb: float = 0
    score_vol_acc: float = 0
    score_oi_div: float = 0
    score_fr: float = 0
    score_lsr: float = 0
    score_liq: float = 0
    score_squeeze: float = 0
    score_momentum: float = 0
    trend_score: float = 0
    squeeze_engine_score: float = 0
    market_mode: str = ""              # TREND / SQUEEZE / HYBRID
    signal_type: str = ""
    vol_ratio: float = 0
    oi_chg_pct: float = 0
    fr: float = 0
    lsr: float = 0
    liq_ratio: float = 0
    price_chg: float = 0
    price_current: float = 0    # Giá hiện tại (close nến gần nhất)
    day_low: float = 0          # Giá thấp nhất trong ngày
    timeframe: str = "1D"       # "1D" hoặc "1H" hoặc "REV"
    reversal_type: str = ""     # "PUMP_REVERSAL" / "DUMP_REVERSAL" / "H1_BREAKOUT_LONG" / "H1_BREAKOUT_SHORT"
    h1_chg: float = 0           # % thay đổi nến 1H gần nhất
    m30_chg: float = 0          # % thay đổi nến M30 gần nhất (xác nhận)
    m30_confirmed: bool = False  # M30 cùng chiều với tín hiệu reversal
    h1_minutes_left: int = 0    # Số phút còn lại đến khi nến H1 đóng
    # TP/SL cho Reversal signals
    entry: float = 0
    sl: float = 0
    tp1: float = 0
    tp2: float = 0
    tp3: float = 0
    rr_tp1: float = 0           # Risk:Reward tới TP1
    rr_tp2: float = 0           # Risk:Reward tới TP2
    details: list = field(default_factory=list)

    @property
    def display_symbol(self) -> str:
        s = self.symbol
        # KuCoin format: BTCUSDTM → BTC
        if s.endswith("USDTM"):
            base = s[:-5]
        elif s.endswith("USDT"):
            base = s[:-4]
        else:
            base = s
        return f"{base} · {self.exchange}"


# ══════════════════════════════════════════════════════════════
# GENERIC HTTP
# ══════════════════════════════════════════════════════════════

def http_get(url: str, params: dict | None = None, headers: dict | None = None, timeout: int = 15) -> Optional[Any]:
    for attempt in range(3):
        try:
            r = get_session().get(url, params=params or {}, headers=headers or {}, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            log.warning(f"GET failed ({attempt+1}/3): {url} — {e}")
            time.sleep(2 ** attempt)
    return None


def cg_get(endpoint: str, params: dict | None = None) -> Optional[Any]:
    url = f"{COINGLASS_BASE}{endpoint}"
    data = http_get(url, params=params or {}, headers=CG_HEADERS)
    if not data:
        return None
    if isinstance(data, dict) and (data.get("code") in (0, "0") or data.get("success")):
        return data.get("data") or data
    if isinstance(data, dict):
        log.warning(f"Coinglass API error {endpoint}: {data.get('msg', data.get('message', 'unknown'))}")
    return None



def bingx_get(endpoint: str, params: dict | None = None) -> Optional[dict]:
    """BingX public API helper."""
    data = http_get(f"{BINGX_BASE}{endpoint}", params=params or {})
    if not data:
        return None
    code = data.get("code", -1)
    if code == 0:
        return data.get("data", {})
    log.warning(f"BingX API error {endpoint}: code={code} msg={data.get('msg','')}")
    return None

def bybit_get(endpoint: str, params: dict | None = None) -> Optional[dict]:
    data = http_get(f"{BYBIT_BASE}{endpoint}", params=params or {})
    if not data:
        return None
    if data.get("retCode") == 0:
        return data.get("result", {})
    log.warning(f"Bybit API error {endpoint}: {data.get('retMsg', 'unknown')}")
    return None


# ══════════════════════════════════════════════════════════════
# SYMBOLS
# ══════════════════════════════════════════════════════════════

def get_binance_symbols() -> list[str]:
    log.info("Fetching Binance USDT Perp symbols...")
    data = http_get(f"{BINANCE_BASE}/fapi/v1/exchangeInfo")
    if not data:
        return []

    symbols = []
    for item in data.get("symbols", []):
        symbol = item.get("symbol", "")
        if (
            item.get("status") == "TRADING"
            and item.get("contractType") == "PERPETUAL"
            and item.get("quoteAsset") == "USDT"
            and symbol.endswith("USDT")
            and symbol not in EXCLUDE_SYMBOLS
        ):
            symbols.append(symbol)

    log.info(f"Found {len(symbols)} Binance symbols")
    return sorted(set(symbols))


def get_bybit_symbols() -> list[str]:
    log.info("Fetching Bybit USDT Perp symbols...")
    symbols: list[str] = []
    cursor = None

    while True:
        params = {
            "category": "linear",
            "status": "Trading",
            "limit": 1000,
        }
        if cursor:
            params["cursor"] = cursor

        data = bybit_get("/v5/market/instruments-info", params)
        if not data:
            break

        for item in data.get("list", []):
            symbol = item.get("symbol", "")
            if (
                symbol.endswith("USDT")
                and item.get("quoteCoin") == "USDT"
                and item.get("contractType") == "LinearPerpetual"
                and symbol not in EXCLUDE_SYMBOLS
            ):
                symbols.append(symbol)

        cursor = data.get("nextPageCursor")
        if not cursor:
            break
        time.sleep(0.1)

    log.info(f"Found {len(symbols)} Bybit symbols")
    return sorted(set(symbols))



def get_bingx_symbols() -> list[str]:
    log.info("Fetching BingX USDT Perp symbols...")
    data = http_get(f"{BINGX_BASE}/openApi/swap/v2/quote/contracts")
    if not data:
        return []
    items = data.get("data", []) if isinstance(data, dict) else []
    symbols = []
    for item in items:
        symbol = item.get("symbol", "").replace("-", "")  # BTC-USDT → BTCUSDT
        if symbol.endswith("USDT") and symbol not in EXCLUDE_SYMBOLS:
            symbols.append(symbol)
    log.info(f"Found {len(symbols)} BingX symbols")
    return sorted(set(symbols))

def get_all_symbols(exchange: str) -> list[str]:
    if exchange == "Binance":
        return get_binance_symbols()
    if exchange == "Bybit":
        return get_bybit_symbols()
    if exchange == "BingX":
        return get_bingx_symbols()
    if exchange == "KuCoin":
        return get_kucoin_symbols()
    raise ValueError(f"Unsupported exchange: {exchange}")


# ══════════════════════════════════════════════════════════════
# KUCOIN API
# ══════════════════════════════════════════════════════════════

def _kucoin_http(url: str, params: dict | None = None, timeout: int = 12) -> Optional[Any]:
    """
    KuCoin HTTP với throttle + 429 backoff.
    Semaphore giới hạn concurrent, delay sau mỗi request, retry dài hơn khi 429.
    """
    with _kucoin_lock:
        for attempt in range(3):
            try:
                r = get_session().get(url, params=params or {}, timeout=timeout)
                if r.status_code == 429:
                    wait = 3.0 * (attempt + 1)   # 3s, 6s, 9s
                    log.debug(f"KuCoin 429 → đợi {wait:.0f}s rồi retry ({attempt+1}/3)...")
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                time.sleep(KUCOIN_REQUEST_DELAY)  # throttle sau mỗi success
                return r.json()
            except requests.RequestException as e:
                if attempt < 2:
                    time.sleep(1.5 * (attempt + 1))
                else:
                    log.debug(f"KuCoin GET failed (3/3): {url} — {e}")
    return None


def _kucoin_parse_response(data: Any) -> Optional[Any]:
    if data is None:
        return None
    if isinstance(data, dict):
        code = str(data.get("code", "200000"))
        if code in ("200000", "0", "200"):
            return data.get("data", data)
        if "data" in data:
            return data["data"]
    return data


def kucoin_get(endpoint: str, params: dict | None = None) -> Optional[Any]:
    """KuCoin Futures public API helper — có throttle."""
    data = _kucoin_http(f"{KUCOIN_BASE}{endpoint}", params=params or {}, timeout=15)
    return _kucoin_parse_response(data)


def kucoin_get_quick(endpoint: str, params: dict | None = None) -> Optional[Any]:
    """KuCoin quick (timeout ngắn hơn) — có throttle."""
    data = _kucoin_http(f"{KUCOIN_BASE}{endpoint}", params=params or {}, timeout=8)
    return _kucoin_parse_response(data)


def get_kucoin_symbols() -> list[str]:
    log.info("Fetching KuCoin USDT Perp symbols...")
    data = kucoin_get("/api/v1/contracts/active")
    if not data or not isinstance(data, list):
        log.warning("KuCoin: không lấy được symbol list")
        return []
    symbols = []
    for s in data:
        sym    = s.get("symbol", "")
        settle = s.get("settleCurrency", "")
        status = s.get("status", "")
        # KuCoin futures symbol: BTCUSDTM (suffix M)
        if settle == "USDT" and status == "Open" and sym.endswith("USDTM"):
            symbols.append(sym)
    log.info(f"Found {len(symbols)} KuCoin USDT Perp symbols")
    return sorted(set(symbols))


def _kucoin_parse_candles(data: Any) -> Optional[list]:
    """KuCoin kline → chuẩn hóa dict. Newest-first → cần reverse sau."""
    if not data or not isinstance(data, list):
        return None
    candles = []
    for row in data:
        try:
            if isinstance(row, list) and len(row) >= 6:
                # [timestamp_ms, open, high, low, close, volume, turnover]
                candles.append({
                    "t": int(row[0]),
                    "o": float(row[1]),
                    "h": float(row[2]),
                    "l": float(row[3]),
                    "c": float(row[4]),
                    "v": float(row[5]),
                })
        except Exception:
            continue
    return list(reversed(candles)) if candles else None  # oldest-first


def get_kucoin_ohlcv(symbol: str, limit: int = 25) -> Optional[list]:
    data = kucoin_get("/api/v1/kline/query", {
        "symbol": symbol,
        "granularity": 1440,   # 1440 phút = 1D
        "limit": limit,
    })
    return _kucoin_parse_candles(data)


def get_kucoin_ohlcv_1h(symbol: str, limit: int = 20) -> Optional[list]:
    data = kucoin_get_quick("/api/v1/kline/query", {
        "symbol": symbol,
        "granularity": 60,
        "limit": limit,
    })
    return _kucoin_parse_candles(data)


def get_kucoin_ohlcv_m30(symbol: str, limit: int = 15) -> Optional[list]:
    data = kucoin_get_quick("/api/v1/kline/query", {
        "symbol": symbol,
        "granularity": 30,
        "limit": limit,
    })
    return _kucoin_parse_candles(data)


def get_kucoin_funding_rate(symbol: str) -> Optional[float]:
    data = kucoin_get_quick(f"/api/v1/funding-rate/{symbol}/current")
    if data and isinstance(data, dict):
        val = data.get("value") or data.get("fundingRate")
        if val is not None:
            return float(val)
    return None


def get_kucoin_oi(symbol: str) -> Optional[float]:
    """OI snapshot từ contract info — KuCoin không có daily hist public."""
    data = kucoin_get_quick(f"/api/v1/contracts/{symbol}")
    if data and isinstance(data, dict):
        oi = data.get("openInterest") or data.get("openInterestValue") or 0
        return float(oi)
    return None
# OHLCV PUBLIC API
# ══════════════════════════════════════════════════════════════

def get_binance_ohlcv(symbol: str, limit: int = 25) -> Optional[list]:
    data = http_get(f"{BINANCE_BASE}/fapi/v1/klines", {
        "symbol": symbol,
        "interval": "1d",
        "limit": limit,
    })
    if not data:
        return None

    candles = []
    for row in data:
        candles.append({
            "t": int(row[0]),
            "o": float(row[1]),
            "h": float(row[2]),
            "l": float(row[3]),
            "c": float(row[4]),
            "v": float(row[5]),
        })
    return candles


def get_bybit_ohlcv(symbol: str, limit: int = 25) -> Optional[list]:
    data = bybit_get("/v5/market/kline", {
        "category": "linear",
        "symbol": symbol,
        "interval": "D",
        "limit": limit,
    })
    if not data:
        return None

    rows = data.get("list", [])
    if not rows:
        return None

    rows = list(reversed(rows))
    candles = []
    for row in rows:
        candles.append({
            "t": int(row[0]),
            "o": float(row[1]),
            "h": float(row[2]),
            "l": float(row[3]),
            "c": float(row[4]),
            "v": float(row[5]),
        })
    return candles



def get_bingx_ohlcv(symbol: str, limit: int = 25) -> Optional[list]:
    """BingX OHLCV — symbol format: BTCUSDT → BTC-USDT cho API."""
    api_symbol = symbol[:-4] + "-USDT" if symbol.endswith("USDT") else symbol
    data = http_get(f"{BINGX_BASE}/openApi/swap/v2/quote/klines", {
        "symbol": api_symbol,
        "interval": "1d",
        "limit": limit,
    })
    if not data:
        return None
    rows = data.get("data", []) if isinstance(data, dict) else data
    if not rows or not isinstance(rows, list):
        return None
    candles = []
    for row in rows:
        try:
            candles.append({
                "t": int(row.get("time", row[0] if isinstance(row, list) else 0)),
                "o": float(row.get("open",  row[1] if isinstance(row, list) else 0)),
                "h": float(row.get("high",  row[2] if isinstance(row, list) else 0)),
                "l": float(row.get("low",   row[3] if isinstance(row, list) else 0)),
                "c": float(row.get("close", row[4] if isinstance(row, list) else 0)),
                "v": float(row.get("volume",row[5] if isinstance(row, list) else 0)),
            })
        except Exception:
            continue
    return candles if candles else None

def get_ohlcv(exchange: str, symbol: str, limit: int = 25) -> Optional[list]:
    if exchange == "Binance":
        return get_binance_ohlcv(symbol, limit)
    if exchange == "Bybit":
        return get_bybit_ohlcv(symbol, limit)
    if exchange == "BingX":
        return get_bingx_ohlcv(symbol, limit)
    if exchange == "KuCoin":
        return get_kucoin_ohlcv(symbol, limit)
    return None


# ── 1H OHLCV ─────────────────────────────────────────────────

def get_binance_ohlcv_1h(symbol: str, limit: int = 20) -> Optional[list]:
    data = http_get_quick(f"{BINANCE_BASE}/fapi/v1/klines", {
        "symbol": symbol,
        "interval": "1h",
        "limit": limit,
    })
    if not data or not isinstance(data, list):
        return None
    candles = []
    for row in data:
        candles.append({
            "t": int(row[0]),
            "o": float(row[1]),
            "h": float(row[2]),
            "l": float(row[3]),
            "c": float(row[4]),
            "v": float(row[5]),
        })
    return candles


def get_bybit_ohlcv_1h(symbol: str, limit: int = 20) -> Optional[list]:
    data = bybit_get("/v5/market/kline", {
        "category": "linear",
        "symbol": symbol,
        "interval": "60",
        "limit": limit,
    })
    if not data:
        return None
    rows = data.get("list", [])
    if not rows:
        return None
    rows = list(reversed(rows))
    return [{"t": int(r[0]), "o": float(r[1]), "h": float(r[2]),
             "l": float(r[3]), "c": float(r[4]), "v": float(r[5])} for r in rows]


def get_bingx_ohlcv_1h(symbol: str, limit: int = 20) -> Optional[list]:
    api_symbol = symbol[:-4] + "-USDT" if symbol.endswith("USDT") else symbol
    data = http_get_quick(f"{BINGX_BASE}/openApi/swap/v2/quote/klines", {
        "symbol": api_symbol,
        "interval": "1h",
        "limit": limit,
    })
    if not data:
        return None
    rows = data.get("data", []) if isinstance(data, dict) else data
    if not rows or not isinstance(rows, list):
        return None
    candles = []
    for row in rows:
        try:
            candles.append({
                "t": int(row.get("time", row[0] if isinstance(row, list) else 0)),
                "o": float(row.get("open",  row[1] if isinstance(row, list) else 0)),
                "h": float(row.get("high",  row[2] if isinstance(row, list) else 0)),
                "l": float(row.get("low",   row[3] if isinstance(row, list) else 0)),
                "c": float(row.get("close", row[4] if isinstance(row, list) else 0)),
                "v": float(row.get("volume",row[5] if isinstance(row, list) else 0)),
            })
        except Exception:
            continue
    return candles if candles else None


def get_ohlcv_1h(exchange: str, symbol: str, limit: int = 20) -> Optional[list]:
    if exchange == "Binance":
        return get_binance_ohlcv_1h(symbol, limit)
    if exchange == "Bybit":
        return get_bybit_ohlcv_1h(symbol, limit)
    if exchange == "BingX":
        return get_bingx_ohlcv_1h(symbol, limit)
    if exchange == "KuCoin":
        return get_kucoin_ohlcv_1h(symbol, limit)
    return None


# ── M30 OHLCV ────────────────────────────────────────────────

def get_binance_ohlcv_m30(symbol: str, limit: int = 15) -> Optional[list]:
    data = http_get_quick(f"{BINANCE_BASE}/fapi/v1/klines", {
        "symbol": symbol,
        "interval": "30m",
        "limit": limit,
    })
    if not data or not isinstance(data, list):
        return None
    return [{"t": int(r[0]), "o": float(r[1]), "h": float(r[2]),
             "l": float(r[3]), "c": float(r[4]), "v": float(r[5])} for r in data]


def get_bybit_ohlcv_m30(symbol: str, limit: int = 15) -> Optional[list]:
    data = bybit_get("/v5/market/kline", {
        "category": "linear",
        "symbol": symbol,
        "interval": "30",
        "limit": limit,
    })
    if not data:
        return None
    rows = list(reversed(data.get("list", [])))
    return [{"t": int(r[0]), "o": float(r[1]), "h": float(r[2]),
             "l": float(r[3]), "c": float(r[4]), "v": float(r[5])} for r in rows] if rows else None


def get_bingx_ohlcv_m30(symbol: str, limit: int = 15) -> Optional[list]:
    api_symbol = symbol[:-4] + "-USDT" if symbol.endswith("USDT") else symbol
    data = http_get_quick(f"{BINGX_BASE}/openApi/swap/v2/quote/klines", {
        "symbol": api_symbol,
        "interval": "30m",
        "limit": limit,
    })
    if not data:
        return None
    rows = data.get("data", []) if isinstance(data, dict) else data
    if not rows or not isinstance(rows, list):
        return None
    candles = []
    for row in rows:
        try:
            candles.append({
                "t": int(row.get("time", row[0] if isinstance(row, list) else 0)),
                "o": float(row.get("open",  row[1] if isinstance(row, list) else 0)),
                "h": float(row.get("high",  row[2] if isinstance(row, list) else 0)),
                "l": float(row.get("low",   row[3] if isinstance(row, list) else 0)),
                "c": float(row.get("close", row[4] if isinstance(row, list) else 0)),
                "v": float(row.get("volume",row[5] if isinstance(row, list) else 0)),
            })
        except Exception:
            continue
    return candles if candles else None


def get_ohlcv_m30(exchange: str, symbol: str, limit: int = 15) -> Optional[list]:
    if exchange == "Binance":
        return get_binance_ohlcv_m30(symbol, limit)
    if exchange == "Bybit":
        return get_bybit_ohlcv_m30(symbol, limit)
    if exchange == "BingX":
        return get_bingx_ohlcv_m30(symbol, limit)
    if exchange == "KuCoin":
        return get_kucoin_ohlcv_m30(symbol, limit)
    return None


# ── H6 / H12 OHLCV (cho MTF Daily scan) ─────────────────────

def _get_ohlcv_interval(exchange: str, symbol: str, interval_binance: str,
                         interval_bybit: str, interval_bingx: str,
                         interval_kucoin_min: int, limit: int = 10) -> Optional[list]:
    """Generic multi-exchange OHLCV fetcher cho H6/H12."""
    if exchange == "Binance":
        data = http_get_quick(f"{BINANCE_BASE}/fapi/v1/klines", {
            "symbol": symbol, "interval": interval_binance, "limit": limit,
        })
        if not data or not isinstance(data, list):
            return None
        return [{"t": int(r[0]), "o": float(r[1]), "h": float(r[2]),
                 "l": float(r[3]), "c": float(r[4]), "v": float(r[5])} for r in data]

    if exchange == "Bybit":
        data = bybit_get("/v5/market/kline", {
            "category": "linear", "symbol": symbol,
            "interval": interval_bybit, "limit": limit,
        })
        if not data:
            return None
        rows = list(reversed(data.get("list", [])))
        return [{"t": int(r[0]), "o": float(r[1]), "h": float(r[2]),
                 "l": float(r[3]), "c": float(r[4]), "v": float(r[5])} for r in rows] if rows else None

    if exchange == "BingX":
        api_symbol = symbol[:-4] + "-USDT" if symbol.endswith("USDT") else symbol
        data = http_get_quick(f"{BINGX_BASE}/openApi/swap/v2/quote/klines", {
            "symbol": api_symbol, "interval": interval_bingx, "limit": limit,
        })
        if not data:
            return None
        rows = data.get("data", []) if isinstance(data, dict) else data
        if not rows:
            return None
        candles = []
        for row in rows:
            try:
                candles.append({
                    "t": int(row.get("time", row[0] if isinstance(row, list) else 0)),
                    "o": float(row.get("open",  row[1] if isinstance(row, list) else 0)),
                    "h": float(row.get("high",  row[2] if isinstance(row, list) else 0)),
                    "l": float(row.get("low",   row[3] if isinstance(row, list) else 0)),
                    "c": float(row.get("close", row[4] if isinstance(row, list) else 0)),
                    "v": float(row.get("volume",row[5] if isinstance(row, list) else 0)),
                })
            except Exception:
                continue
        return candles if candles else None

    if exchange == "KuCoin":
        return _kucoin_parse_candles(
            kucoin_get_quick("/api/v1/kline/query", {
                "symbol": symbol, "granularity": interval_kucoin_min, "limit": limit,
            })
        )
    return None


def get_ohlcv_h6(exchange: str, symbol: str, limit: int = 10) -> Optional[list]:
    return _get_ohlcv_interval(exchange, symbol, "6h", "360", "6h", 360, limit)


def get_ohlcv_h12(exchange: str, symbol: str, limit: int = 6) -> Optional[list]:
    return _get_ohlcv_interval(exchange, symbol, "12h", "720", "12h", 720, limit)


def get_ohlcv_h2(exchange: str, symbol: str, limit: int = 15) -> Optional[list]:
    return _get_ohlcv_interval(exchange, symbol, "2h", "120", "2h", 120, limit)


# ══════════════════════════════════════════════════════════════
# FUNDING / OI / LSR / LIQUIDATION
# Ưu tiên Binance/Bybit public API để tránh Coinglass bị 500.
# Coinglass chỉ dùng phụ cho Liquidation nếu bật USE_COINGLASS_LIQ.
# ══════════════════════════════════════════════════════════════

USE_COINGLASS_LIQ = True   # Bật để lấy liquidation — quan trọng cho scoring


def http_get_quick(url: str, params: dict | None = None, headers: dict | None = None, timeout: int = 8) -> Optional[Any]:
    """GET nhanh, chỉ thử 1 lần. Dùng cho endpoint phụ để bot không bị kẹt."""
    try:
        r = get_session().get(url, params=params or {}, headers=headers or {}, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        log.debug(f"Quick GET failed: {url} — {e}")
        return None


def cg_get_quick(endpoint: str, params: dict | None = None) -> Optional[Any]:
    """Coinglass optional: fail thì bỏ qua ngay, không retry 3 lần."""
    if not COINGLASS_API_KEY:
        return None
    url = f"{COINGLASS_BASE}{endpoint}"
    data = http_get_quick(url, params=params or {}, headers=CG_HEADERS)
    if not data:
        return None
    if isinstance(data, dict) and (data.get("code") in (0, "0") or data.get("success")):
        return data.get("data") or data
    return None


def get_funding_rate(exchange: str, symbol: str) -> Optional[float]:
    """Funding lấy trực tiếp từ Binance/Bybit public API."""
    if exchange == "Binance":
        d = http_get_quick(f"{BINANCE_BASE}/fapi/v1/premiumIndex", {"symbol": symbol})
        if d and "lastFundingRate" in d:
            return float(d["lastFundingRate"])

    elif exchange == "Bybit":
        d = bybit_get("/v5/market/tickers", {"category": "linear", "symbol": symbol})
        rows = d.get("list", []) if d else []
        if rows and "fundingRate" in rows[0]:
            return float(rows[0]["fundingRate"])

    elif exchange == "BingX":
        api_symbol = symbol[:-4] + "-USDT" if symbol.endswith("USDT") else symbol
        d = http_get_quick(f"{BINGX_BASE}/openApi/swap/v2/quote/premiumIndex",
                           {"symbol": api_symbol})
        if d and isinstance(d, dict):
            rows = d.get("data", {})
            if isinstance(rows, dict) and "lastFundingRate" in rows:
                return float(rows["lastFundingRate"])

    elif exchange == "KuCoin":
        return get_kucoin_funding_rate(symbol)

    return None


def get_oi_history(exchange: str, symbol: str, limit: int = 6) -> Optional[list]:
    """OI history lấy trực tiếp từ Binance/Bybit public API."""
    if exchange == "Binance":
        d = http_get_quick(f"{BINANCE_BASE}/futures/data/openInterestHist", {
            "symbol": symbol,
            "period": "1d",
            "limit": limit,
        })
        if d and isinstance(d, list):
            out = []
            for x in d:
                # sumOpenInterestValue = USD notional; ổn hơn để so sánh % thay đổi
                val = x.get("sumOpenInterestValue") or x.get("sumOpenInterest") or 0
                out.append({"openInterest": float(val)})
            return out

    elif exchange == "Bybit":
        d = bybit_get("/v5/market/open-interest", {
            "category": "linear",
            "symbol": symbol,
            "intervalTime": "1d",
            "limit": limit,
        })
        rows = d.get("list", []) if d else []
        if rows:
            rows = list(reversed(rows))
            return [{"openInterest": float(x.get("openInterest", 0))} for x in rows]

    elif exchange == "BingX":
        api_symbol = symbol[:-4] + "-USDT" if symbol.endswith("USDT") else symbol
        d = http_get_quick(f"{BINGX_BASE}/openApi/swap/v2/quote/openInterestHist", {
            "symbol": api_symbol,
            "period": "1d",
            "limit": limit,
        })
        if d and isinstance(d, dict):
            rows = d.get("data", [])
            if isinstance(rows, list) and rows:
                return [{"openInterest": float(x.get("openInterest", 0))} for x in rows]

    elif exchange == "KuCoin":
        # KuCoin không có daily OI hist public → dùng snapshot
        # oi_change sẽ = 0 nhưng vẫn có OI tuyệt đối để tham khảo
        oi = get_kucoin_oi(symbol)
        if oi is not None:
            return [{"openInterest": oi}] * limit

    return None


def get_lsr(exchange: str, symbol: str) -> Optional[float]:
    """Long/Short Ratio. Binance có public endpoint; Bybit bỏ qua nếu không có."""
    if exchange == "Binance":
        d = http_get_quick(f"{BINANCE_BASE}/futures/data/globalLongShortAccountRatio", {
            "symbol": symbol,
            "period": "1d",
            "limit": 1,
        })
        if d and isinstance(d, list) and d:
            return float(d[-1].get("longShortRatio", 0))

    # Optional Coinglass fallback nếu cần
    data = cg_get_quick("/futures/global-long-short-account-ratio/history", {
        "exchange": exchange,
        "symbol": symbol,
        "interval": "1d",
        "limit": 1,
    })
    if data and isinstance(data, list) and len(data) > 0:
        return float(data[0].get("longShortRatio", 0))

    # BingX: không có LSR public endpoint → trả None
    return None


def get_liquidation(exchange: str, symbol: str) -> tuple[float, float]:
    """Liquidation chỉ lấy nếu bật USE_COINGLASS_LIQ, fail thì trả 0 để bot không kẹt."""
    if not USE_COINGLASS_LIQ:
        return 0.0, 0.0

    data = cg_get_quick("/futures/liquidation/history", {
        "exchange": exchange,
        "symbol": symbol,
        "interval": "1d",
        "limit": 1,
    })
    if data and isinstance(data, list) and len(data) > 0:
        longs = float(data[0].get("longLiquidationUsd", 0))
        shorts = float(data[0].get("shortLiquidationUsd", 0))
        return longs, shorts
    return 0.0, 0.0


# ══════════════════════════════════════════════════════════════
# DATA FETCHER
# ══════════════════════════════════════════════════════════════

def fetch_coin_data(exchange: str, symbol: str) -> Optional[CoinData]:
    coin = CoinData(symbol=symbol, exchange=exchange)

    candles = get_ohlcv(exchange, symbol, limit=25)
    if not candles or len(candles) < 11:
        return None

    latest = candles[-1]
    coin.open = float(latest.get("o", 0))
    coin.high = float(latest.get("h", 0))
    coin.low = float(latest.get("l", 0))
    coin.close = float(latest.get("c", 0))
    coin.volume = float(latest.get("v", 0))

    if coin.close <= 0 or coin.open <= 0:
        return None

    coin.price_change_pct = (coin.close - coin.open) / coin.open * 100

    # Lookback 2 nến ngày trước — cho Reversal Engine
    if len(candles) >= 3:
        c1 = candles[-2]  # hôm qua
        o1, c1c = float(c1.get("o", 0)), float(c1.get("c", 0))
        if o1 > 0:
            coin.prev1d_change_pct = (c1c - o1) / o1 * 100
    if len(candles) >= 4:
        c2 = candles[-3]  # hôm kia
        o2, c2c = float(c2.get("o", 0)), float(c2.get("c", 0))
        if o2 > 0:
            coin.prev2d_change_pct = (c2c - o2) / o2 * 100

    # Intraday dump depth: (open - low) / open — nến ngày hiện tại
    # Case MLNUSDT: open=3.157, low=2.073 → dump 34.3% trong ngày dù close chưa phản ánh hết
    if coin.open > 0 and coin.low > 0:
        coin.intraday_dump_pct = (coin.open - coin.low) / coin.open * 100
    prev_vols = [float(c.get("v", 0)) for c in candles[-11:-1]]
    coin.vol_ma10 = sum(prev_vols) / len(prev_vols) if prev_vols else 0

    lows_20 = [float(c.get("l", 0)) for c in candles[-21:-1]]
    coin.low_20d = min(lows_20) if lows_20 else 0

    fr = get_funding_rate(exchange, symbol)
    coin.funding_rate = fr if fr is not None else 0
    # Không sleep ở đây: bản FAST dùng ThreadPool + timeout ngắn

    oi_hist = get_oi_history(exchange, symbol, limit=6)
    if oi_hist and len(oi_hist) >= 5:
        coin.oi_current = float(oi_hist[-1].get("openInterest", 0))
        coin.oi_prev4 = float(oi_hist[-5].get("openInterest", 0))
        if coin.oi_prev4 > 0:
            coin.oi_change_pct = (coin.oi_current - coin.oi_prev4) / coin.oi_prev4 * 100
    # Không sleep ở đây: bản FAST dùng ThreadPool + timeout ngắn

    lsr = get_lsr(exchange, symbol)
    coin.lsr = lsr if lsr is not None else 0
    # Không sleep ở đây: bản FAST dùng ThreadPool + timeout ngắn

    longs_liq, shorts_liq = get_liquidation(exchange, symbol)
    coin.liq_longs = longs_liq
    coin.liq_shorts = shorts_liq
    if longs_liq > 0:
        coin.liq_ratio = shorts_liq / longs_liq
    elif shorts_liq > 0:
        coin.liq_ratio = 99.0

    # ── 1H data cho Reversal Engine ──────────────────────────────
    if ENABLE_1H_REVERSAL:
        h1_candles = get_ohlcv_1h(exchange, symbol, limit=20)
        if h1_candles and len(h1_candles) >= 12:
            h1_latest = h1_candles[-1]
            coin.h1_open   = float(h1_latest.get("o", 0))
            coin.h1_close  = float(h1_latest.get("c", 0))
            coin.h1_high   = float(h1_latest.get("h", 0))
            coin.h1_low    = float(h1_latest.get("l", 0))
            coin.h1_volume = float(h1_latest.get("v", 0))
            if coin.h1_open > 0:
                coin.h1_price_change_pct = (coin.h1_close - coin.h1_open) / coin.h1_open * 100
            prev_h1_vols = [float(c.get("v", 0)) for c in h1_candles[-11:-1]]
            coin.h1_vol_ma10 = sum(prev_h1_vols) / len(prev_h1_vols) if prev_h1_vols else 0
            coin.h1_available = True

    # ── M30 data — xác nhận momentum cho reversal scan ───────────
    if ENABLE_1H_REVERSAL:
        m30_candles = get_ohlcv_m30(exchange, symbol, limit=15)
        if m30_candles and len(m30_candles) >= 12:
            # m30[-1] = nến M30 đang mở (hoặc vừa đóng)
            # m30[-2] = nến M30 đã đóng gần nhất
            m30_last   = m30_candles[-1]
            m30_prev   = m30_candles[-2]
            coin.m30_open   = float(m30_last.get("o", 0))
            coin.m30_close  = float(m30_last.get("c", 0))
            coin.m30_high   = float(m30_last.get("h", 0))
            coin.m30_low    = float(m30_last.get("l", 0))
            coin.m30_volume = float(m30_last.get("v", 0))
            if coin.m30_open > 0:
                coin.m30_price_change_pct = (coin.m30_close - coin.m30_open) / coin.m30_open * 100
            prev_m30_o = float(m30_prev.get("o", 0))
            prev_m30_c = float(m30_prev.get("c", 0))
            if prev_m30_o > 0:
                coin.m30_prev_change_pct = (prev_m30_c - prev_m30_o) / prev_m30_o * 100
            prev_m30_vols = [float(c.get("v", 0)) for c in m30_candles[-11:-1]]
            coin.m30_vol_ma10 = sum(prev_m30_vols) / len(prev_m30_vols) if prev_m30_vols else 0
            coin.m30_available = True

    return coin


# ══════════════════════════════════════════════════════════════
# SCORING ENGINE
# ══════════════════════════════════════════════════════════════


def classify_market_mode(result: ScoreResult, coin: CoinData, vol_ratio: float) -> None:
    """Tách coin thành 3 mode:
    - TREND: kiểu IRYS, OI + momentum + L/S khỏe, bền hơn
    - SQUEEZE: kiểu COS, funding âm sâu + crowded + volume burst, chạy violent hơn
    - HYBRID: vừa có trend vừa có squeeze fuel
    """
    fr_pct = coin.funding_rate * 100

    trend = 0.0
    squeeze = 0.0

    # TREND ENGINE: ưu tiên price expansion + OI expansion + L/S không quá crowded
    if coin.price_change_pct >= 20:
        trend += 2.0
    elif coin.price_change_pct >= 12:
        trend += 1.5
    elif coin.price_change_pct >= 7:
        trend += 1.0

    if coin.oi_change_pct >= 50:
        trend += 3.0
    elif coin.oi_change_pct >= 20:
        trend += 2.0
    elif coin.oi_change_pct >= OI_DIV_MIN_PCT:
        trend += 1.0

    if vol_ratio >= 3:
        trend += 1.5
    elif vol_ratio >= 2:
        trend += 1.0
    elif vol_ratio >= 1.5:
        trend += 0.5

    if 0 < coin.lsr <= MAX_LSR_HEALTHY:
        trend += 1.0

    if -0.15 <= fr_pct <= 0.05:
        trend += 0.5

    # SQUEEZE ENGINE: ưu tiên funding âm sâu + crowd + burst
    if fr_pct <= -0.70:
        squeeze += 3.0
    elif fr_pct <= FR_SQUEEZE_THRESHOLD:
        squeeze += 2.5
    elif fr_pct < -0.15:
        squeeze += 1.0

    if coin.lsr >= 2.7:
        squeeze += 2.0
    elif coin.lsr >= MAX_LSR_HEALTHY:
        squeeze += 1.5

    if vol_ratio >= 4:
        squeeze += 2.0
    elif vol_ratio >= 2:
        squeeze += 1.2

    if coin.price_change_pct >= 20:
        squeeze += 1.5
    elif coin.price_change_pct >= 10:
        squeeze += 1.0

    if coin.oi_change_pct >= OI_DIV_MIN_PCT:
        squeeze += 1.0

    result.trend_score = round(trend, 1)
    result.squeeze_engine_score = round(squeeze, 1)

    if trend >= HYBRID_MIN_SCORE and squeeze >= HYBRID_MIN_SCORE:
        result.market_mode = "HYBRID"
    elif squeeze >= SQUEEZE_MIN_SCORE:
        result.market_mode = "SQUEEZE"
    elif trend >= TREND_MIN_SCORE:
        result.market_mode = "TREND"
    else:
        result.market_mode = "MOMENTUM"

    # Bonus nhỏ để rank đúng kiểu market hiện tại: COS violent squeeze vẫn có thể vượt trend nếu đủ mạnh.
    if result.market_mode == "HYBRID":
        result.total_score += 0.7
    elif result.market_mode == "SQUEEZE":
        result.total_score += 0.5
    elif result.market_mode == "TREND":
        result.total_score += 0.4

def calc_pump_tp_sl(result: ScoreResult, coin: CoinData) -> None:
    """Tính Entry/SL/TP cho pump/dump từ range nến D."""
    import math
    d_range = coin.high - coin.low
    if d_range <= 0 or coin.close <= 0:
        return

    def fmt(v: float) -> float:
        if v <= 0: return 0.0
        digits = max(2, -int(math.floor(math.log10(abs(v)))) + 3)
        return round(v, digits)

    entry = coin.close
    result.entry = fmt(entry)

    if coin.close >= coin.open:  # PUMP
        result.sl  = fmt(coin.low  - d_range * 0.1)
        result.tp1 = fmt(entry + d_range * 0.5)
        result.tp2 = fmt(entry + d_range * 1.0)
        result.tp3 = fmt(entry + d_range * 1.618)
    else:  # DUMP
        result.sl  = fmt(coin.high + d_range * 0.1)
        result.tp1 = fmt(entry - d_range * 0.5)
        result.tp2 = fmt(entry - d_range * 1.0)
        result.tp3 = fmt(entry - d_range * 1.618)


def score_coin_pump(coin: CoinData) -> Optional[ScoreResult]:
    """Score coin tiềm năng PUMP (nến xanh, momentum tăng)."""
    # Chỉ xét nến xanh cho PUMP
    if coin.close < coin.open:
        return None

    result = ScoreResult(symbol=coin.symbol, exchange=coin.exchange)
    details = []

    if coin.vol_ma10 <= 0:
        return None

    vol_ratio = coin.volume / coin.vol_ma10
    result.vol_ratio = round(vol_ratio, 2)
    result.oi_chg_pct = round(coin.oi_change_pct, 1)
    result.fr = round(coin.funding_rate * 100, 4)
    result.lsr = round(coin.lsr, 4)
    result.liq_ratio = round(coin.liq_ratio, 2)
    result.price_chg    = round(coin.price_change_pct, 2)
    result.price_current = round(coin.close, 8)
    result.day_low       = round(coin.low,   8)

    # Filter noise: bỏ coin tăng yếu + volume yếu để tránh lọt top kiểu 1INCH/MOG vol thấp
    if vol_ratio < MIN_VOL_RATIO_FILTER and coin.price_change_pct < MIN_PRICE_CHANGE_FILTER:
        return None

    chg = coin.price_change_pct

    # 0. Momentum — 2 tier:
    #
    #   Tier 1 (vol xác nhận): cả price lẫn vol đều mạnh → pump bền, điểm cao nhất
    #   Tier 2 (thin air):     pump mạnh dù vol thấp hơn MA → vẫn alert, điểm thấp hơn 0.5đ
    #   Ví dụ: VELVET +19.8% vol 0.2x, AIN +34.7% vol 0.29x, QUSDT +34% vol 0.29x
    #          PIEVERSEUSDT +26.4% vol 1.58x → tất cả đáng alert

    if chg >= 20 and vol_ratio >= 2:
        result.score_momentum = 3.0
        details.append(f"🚀 Momentum mạnh (+{chg:.1f}% vol {vol_ratio:.1f}x)")
    elif chg >= 12 and vol_ratio >= 1.5:
        result.score_momentum = 2.0
        details.append(f"🚀 Momentum (+{chg:.1f}% vol {vol_ratio:.1f}x)")
    elif chg >= 30:                          # thin air pump cực mạnh (≥30%)
        result.score_momentum = 2.5
        details.append(f"🚀 Thin-air pump cực mạnh (+{chg:.1f}%)")
    elif chg >= 20:                          # thin air pump mạnh (20-30%)
        result.score_momentum = 2.0
        details.append(f"🚀 Thin-air pump (+{chg:.1f}%)")
    elif chg >= 12:                          # thin air pump vừa (12-20%)
        result.score_momentum = 1.5
        details.append(f"📈 Thin-air pump vừa (+{chg:.1f}%)")
    elif chg >= 7:
        result.score_momentum = 1.0
        details.append(f"📈 Giá tăng (+{chg:.1f}%)")

    # 1. Cold Volume Burst
    price_near_bottom = coin.low_20d > 0 and coin.close <= coin.low_20d * (1 + BOTTOM_PCT / 100)
    if vol_ratio >= VOL_SPIKE_MIN * 1.5 and price_near_bottom:
        result.score_cvb = 3.0
        details.append(f"🌋 CVB mạnh ({vol_ratio:.1f}x)")
    elif vol_ratio >= VOL_SPIKE_MIN and price_near_bottom:
        result.score_cvb = 2.0
        details.append(f"🌋 CVB ({vol_ratio:.1f}x)")
    elif vol_ratio >= VOL_SPIKE_MIN:
        result.score_cvb = 1.0
        details.append(f"📊 Vol Spike ({vol_ratio:.1f}x)")
    elif vol_ratio < VOL_SPIKE_MIN and chg >= 12:
        # Thin-air: vol thấp hơn MA nhưng pump mạnh
        # Bù bằng OI tăng → xác nhận có lực mua thật
        if coin.oi_change_pct >= 15:
            result.score_cvb = 1.5
            details.append(f"🌬️ Thin-air + OI {coin.oi_change_pct:.1f}% (vol {vol_ratio:.2f}x)")
        elif coin.oi_change_pct >= 8:
            result.score_cvb = 0.8
            details.append(f"🌬️ Thin-air vol {vol_ratio:.2f}x")

    # 2. OI Divergence
    if coin.oi_change_pct >= 50 and coin.oi_change_pct > chg:
        result.score_oi_div = 3.0
        details.append(f"📡 OI Div cực mạnh (+{coin.oi_change_pct:.1f}%)")
    elif coin.oi_change_pct >= OI_DIV_MIN_PCT and coin.oi_change_pct > chg * 1.5:
        result.score_oi_div = 2.0
        details.append(f"📡 OI Div mạnh (+{coin.oi_change_pct:.1f}%)")
    elif coin.oi_change_pct >= OI_DIV_MIN_PCT:
        result.score_oi_div = 1.0
        details.append(f"📡 OI tăng (+{coin.oi_change_pct:.1f}%)")

    # 3. Funding Rate
    fr_pct = coin.funding_rate * 100
    if fr_pct <= FR_SQUEEZE_THRESHOLD:
        result.score_fr = 1.5
        result.score_squeeze = 0.5
        details.append(f"💥 FR âm sâu ({fr_pct:.3f}%)")
    elif fr_pct <= FR_MAX_NORMAL:
        result.score_fr = 1.0
        details.append(f"💰 FR thấp ({fr_pct:.4f}%)")
    elif fr_pct <= 0.05:
        result.score_fr = 0.5
        details.append(f"💰 FR OK ({fr_pct:.4f}%)")
    else:
        details.append(f"⚠️ FR cao ({fr_pct:.4f}%)")

    # 4. Long/Short Ratio
    if LSR_MIN <= coin.lsr <= MAX_LSR_HEALTHY:
        result.score_lsr = 1.0
        details.append(f"📈 L/S healthy {coin.lsr:.4f} ✅")
    elif coin.lsr > MAX_LSR_HEALTHY:
        result.score_lsr = -0.5
        details.append(f"⚠️ L/S crowded {coin.lsr:.4f}")
    elif coin.lsr > 0:
        details.append(f"📉 L/S {coin.lsr:.4f}")

    # 5. Liquidation direction (pump: shorts bị liq nhiều hơn → tốt)
    if coin.liq_ratio >= LIQ_RATIO_MIN_GOOD * 2:
        result.score_liq = 2.0
        details.append(f"💥 Shorts liq {coin.liq_ratio:.1f}x Longs")
    elif coin.liq_ratio >= LIQ_RATIO_MIN_GOOD:
        result.score_liq = 1.0
        details.append(f"✅ Shorts liq {coin.liq_ratio:.1f}x Longs")
    elif 0 < coin.liq_ratio < 0.5:
        result.score_liq = -1.0
        details.append(f"❌ Longs liq {1 / coin.liq_ratio:.1f}x Shorts")
    elif coin.liq_longs > 0:
        longs_x = coin.liq_longs / max(coin.liq_shorts, 1)
        if longs_x >= 5:
            result.score_liq = -2.0
            details.append(f"❌❌ Longs liq {longs_x:.0f}x (bull trap)")

    # 6. Nến đẹp
    candle_body = abs(coin.close - coin.open)
    upper_wick = coin.high - coin.close
    if candle_body > 0 and upper_wick < candle_body * 0.5:
        result.total_score += 0.5
        details.append("✅ Nến đẹp")

    # 7. Vol-confirmed bonus — bù khi OI thấp/không có data
    #    Vol ≥ 2x + pump ≥ 12% mà không có OI signal = lực mua thật từ spot/market
    #    Ví dụ: UBUSDT +15.38% vol 2.1x OI +8.5% → vừa miss, cần bonus này
    if vol_ratio >= 2.0 and chg >= 12 and result.score_oi_div == 0:
        result.total_score += 0.5
        details.append(f"📊 Vol-confirmed ({vol_ratio:.1f}x) bù OI thấp")
    elif vol_ratio >= 3.0 and chg >= 20 and result.score_oi_div <= 1.0:
        result.total_score += 0.3
        details.append(f"📊 Vol mạnh ({vol_ratio:.1f}x)")

    result.total_score += (
        result.score_momentum + result.score_cvb + result.score_oi_div + result.score_fr +
        result.score_lsr + result.score_liq + result.score_squeeze
    )

    # Trend/Squeeze engine classification
    classify_market_mode(result, coin, vol_ratio)
    if result.market_mode == "TREND":
        details.insert(0, f"🟢 TREND engine {result.trend_score:.1f}")
    elif result.market_mode == "SQUEEZE":
        details.insert(0, f"🔴 SQUEEZE engine {result.squeeze_engine_score:.1f}")
    elif result.market_mode == "HYBRID":
        details.insert(0, f"🟣 HYBRID T{result.trend_score:.1f}/S{result.squeeze_engine_score:.1f}")

    if result.market_mode == "HYBRID":
        result.signal_type = "🟣 HYBRID TREND+SQUEEZE"
    elif result.market_mode == "SQUEEZE":
        result.signal_type = "🔴 SQUEEZE ENGINE"
    elif result.market_mode == "TREND":
        result.signal_type = "🟢 TREND ENGINE"
    elif result.score_momentum >= 2 and result.score_oi_div >= 2:
        result.signal_type = "🚀📡 MOMENTUM+OI"
    elif result.score_squeeze > 0 and result.score_cvb > 0:
        result.signal_type = "💥🌋 SQUEEZE+BURST"
    elif result.score_squeeze > 0:
        result.signal_type = "💥 SHORT SQUEEZE"
    elif result.score_cvb >= 2 and result.score_oi_div >= 1:
        result.signal_type = "🌋📡 CVB+OI DIV"
    elif result.score_cvb >= 2:
        result.signal_type = "🌋 CVB"
    elif result.score_oi_div >= 2:
        result.signal_type = "📡 OI DIVERGENCE"
    elif result.score_fr >= 2 and result.score_lsr >= 1:
        result.signal_type = "📈 STEADY GRIND"
    else:
        result.signal_type = "⚡ PUMP"

    result.details = details
    if result.total_score < MIN_SCORE:
        return None
    calc_pump_tp_sl(result, coin)
    return result
    """Score coin tiềm năng DUMP (nến đỏ, momentum giảm mạnh)."""
    # Chỉ xét nến đỏ cho DUMP
    if coin.close >= coin.open:
        return None

    result = ScoreResult(symbol=coin.symbol, exchange=coin.exchange)
    details = []

    if coin.vol_ma10 <= 0:
        return None

    vol_ratio = coin.volume / coin.vol_ma10
    result.vol_ratio = round(vol_ratio, 2)
    result.oi_chg_pct = round(coin.oi_change_pct, 1)
    result.fr = round(coin.funding_rate * 100, 4)
    result.lsr = round(coin.lsr, 4)
    result.liq_ratio = round(coin.liq_ratio, 2)
    result.price_chg    = round(coin.price_change_pct, 2)  # âm
    result.price_current = round(coin.close, 8)
    result.day_low       = round(coin.low,   8)

    drop_pct = abs(coin.price_change_pct)

    # Filter noise: dùng ngưỡng dump riêng (thấp hơn pump)
    if vol_ratio < MIN_DUMP_VOL_RATIO and drop_pct < MIN_DUMP_PRICE_DROP:
        return None
    if drop_pct < MIN_DUMP_PRICE_DROP:
        return None

    # 0. Momentum giảm mạnh
    if drop_pct >= 20 and vol_ratio >= 2:
        result.score_momentum = 3.0
        details.append(f"📉 Dump mạnh (-{drop_pct:.1f}%)")
    elif drop_pct >= 12 and vol_ratio >= 1.5:
        result.score_momentum = 2.0
        details.append(f"📉 Dump (-{drop_pct:.1f}%)")
    elif drop_pct >= 7:
        result.score_momentum = 1.5
        details.append(f"🔻 Giảm mạnh (-{drop_pct:.1f}%)")
    elif drop_pct >= 5:
        result.score_momentum = 1.0
        details.append(f"🔻 Giảm vừa (-{drop_pct:.1f}%)")
    elif drop_pct >= 3:
        result.score_momentum = 0.5
        details.append(f"🔻 Giảm (-{drop_pct:.1f}%)")

    # 1. Volume Spike khi dump = panic selling
    if vol_ratio >= VOL_SPIKE_MIN * 1.5:
        result.score_cvb = 3.0
        details.append(f"🌋 Panic Vol mạnh ({vol_ratio:.1f}x)")
    elif vol_ratio >= VOL_SPIKE_MIN:
        result.score_cvb = 2.0
        details.append(f"🌋 Panic Vol ({vol_ratio:.1f}x)")
    elif vol_ratio >= MIN_VOL_RATIO_FILTER:
        result.score_cvb = 1.0
        details.append(f"📊 Vol tăng ({vol_ratio:.1f}x)")

    # 2. OI tăng khi giá giảm = thêm short mới vào → dump tiếp
    abs_price_chg = abs(coin.price_change_pct)
    if coin.oi_change_pct >= 50 and coin.oi_change_pct > abs_price_chg:
        result.score_oi_div = 3.0
        details.append(f"📡 OI Short Crowded cực mạnh (+{coin.oi_change_pct:.1f}%)")
    elif coin.oi_change_pct >= OI_DIV_MIN_PCT and coin.oi_change_pct > abs_price_chg * 1.5:
        result.score_oi_div = 2.0
        details.append(f"📡 OI Short mạnh (+{coin.oi_change_pct:.1f}%)")
    elif coin.oi_change_pct >= OI_DIV_MIN_PCT:
        result.score_oi_div = 1.0
        details.append(f"📡 OI tăng (+{coin.oi_change_pct:.1f}%)")
    # OI giảm khi giá giảm = long đang thoát → dump tiếp
    elif coin.oi_change_pct <= -10:
        result.score_oi_div = 2.0
        details.append(f"📡 Long tháo chạy (OI {coin.oi_change_pct:.1f}%)")
    elif coin.oi_change_pct <= -5:
        result.score_oi_div = 1.0
        details.append(f"📡 OI giảm ({coin.oi_change_pct:.1f}%)")

    # 3. Funding Rate cao dương = long trả phí = áp lực dump
    fr_pct = coin.funding_rate * 100
    if fr_pct >= 0.15:
        result.score_fr = 2.0
        details.append(f"💥 FR dương cao ({fr_pct:.3f}%) — long trap")
    elif fr_pct >= 0.05:
        result.score_fr = 1.0
        details.append(f"⚠️ FR dương ({fr_pct:.4f}%)")
    elif fr_pct >= 0:
        result.score_fr = 0.5
        details.append(f"💰 FR thấp ({fr_pct:.4f}%)")
    else:
        details.append(f"💰 FR âm ({fr_pct:.4f}%) — giảm tín hiệu dump")

    # 4. Long/Short Ratio cao = đám đông long = crowded = dễ dump tiếp
    if coin.lsr >= 2.7:
        result.score_lsr = 2.0
        details.append(f"🐂 L/S quá đông long {coin.lsr:.4f} — dump fuel")
    elif coin.lsr >= MAX_LSR_HEALTHY:
        result.score_lsr = 1.0
        details.append(f"⚠️ L/S crowded {coin.lsr:.4f}")
    elif 0 < coin.lsr <= MAX_LSR_HEALTHY:
        result.score_lsr = -0.5
        details.append(f"📉 L/S healthy {coin.lsr:.4f} — giảm tín hiệu dump")

    # 5. Liquidation: longs bị liq nhiều hơn → dump mạnh
    if coin.liq_longs > 0:
        longs_x = coin.liq_longs / max(coin.liq_shorts, 1)
        if longs_x >= 5:
            result.score_liq = 3.0
            details.append(f"💥💥 Longs liq {longs_x:.0f}x (cascade dump)")
        elif longs_x >= 2:
            result.score_liq = 2.0
            details.append(f"💥 Longs liq {longs_x:.1f}x Shorts")
        elif coin.liq_ratio < 0.5 and coin.liq_ratio > 0:
            result.score_liq = 1.0
            details.append(f"✅ Longs liq nhiều hơn")
    if coin.liq_ratio >= LIQ_RATIO_MIN_GOOD * 2:
        result.score_liq = max(result.score_liq, -1.0)
        details.append(f"❌ Shorts liq {coin.liq_ratio:.1f}x (chống dump)")

    # 6. Nến đỏ dài thân (giảm mạnh, bóng trên ngắn) — xác nhận dump
    candle_body = abs(coin.close - coin.open)
    upper_wick = coin.high - coin.open   # bóng trên tính từ open (vì đây là nến đỏ)
    if candle_body > 0 and upper_wick < candle_body * 0.3:
        result.total_score += 0.5
        details.append("✅ Nến đỏ dài thân")

    result.total_score += (
        result.score_momentum + result.score_cvb + result.score_oi_div + result.score_fr +
        result.score_lsr + result.score_liq
    )

    # Xác định signal
    if result.score_momentum >= 2 and result.score_oi_div >= 2:
        result.signal_type = "📉📡 DUMP+OI SHORT"
    elif result.score_momentum >= 2 and result.score_fr >= 1.5:
        result.signal_type = "📉💥 LONG TRAP DUMP"
    elif result.score_cvb >= 2 and result.score_liq >= 2:
        result.signal_type = "🌋💥 PANIC SELL CASCADE"
    elif result.score_lsr >= 2 and result.score_fr >= 1:
        result.signal_type = "🐂📉 LONG CROWDED DUMP"
    elif result.score_cvb >= 2:
        result.signal_type = "🌋 PANIC VOLUME"
    elif result.score_oi_div >= 2:
        result.signal_type = "📡 SHORT BUILDUP"
    else:
        result.signal_type = "⬇️ DUMP"

    result.market_mode = "DUMP"
    result.details = details
    if result.total_score < MIN_DUMP_SCORE:
        return None
    calc_pump_tp_sl(result, coin)
    return result


def score_coin(coin: CoinData) -> Optional[ScoreResult]:
    """Wrapper: trả về pump score (tương thích ngược với --test-one)."""
    return score_coin_pump(coin)


def calc_reversal_tp(result: ScoreResult, coin: CoinData) -> None:
    """
    Tính Entry / SL / TP1 / TP2 / TP3 dựa trên momentum nến 1H.

    DUMP_REVERSAL (Long):
      Entry = h1_close
      Range = h1_high - h1_low
      SL    = h1_low - range * 0.1        (dưới đáy nến 1H 10% range)
      TP1   = entry + range * 0.5          (50% range)
      TP2   = entry + range * 1.0          (100% range = full nến)
      TP3   = entry + range * 1.618        (Fibonacci extension)
      Nếu momentum mạnh (h1_chg ≥ 8%) → nhân thêm 1.2x cho TP2/TP3
      Nếu vol spike mạnh (h1_vol_ratio ≥ 3x) → nhân thêm 1.1x tất cả TP

    PUMP_REVERSAL (Short):
      Entry = h1_close
      Range = h1_high - h1_low
      SL    = h1_high + range * 0.1        (trên đỉnh nến 1H 10% range)
      TP1   = entry - range * 0.5
      TP2   = entry - range * 1.0
      TP3   = entry - range * 1.618
    """
    if coin.h1_high <= 0 or coin.h1_low <= 0 or coin.h1_close <= 0:
        return

    h1_range = coin.h1_high - coin.h1_low
    if h1_range <= 0:
        return

    h1_vol_ratio = coin.h1_volume / coin.h1_vol_ma10 if coin.h1_vol_ma10 > 0 else 0
    h1_chg = abs(coin.h1_price_change_pct)

    # Momentum multiplier — nến càng mạnh thì TP xa hơn
    mom_mult = 1.0
    if h1_chg >= 8:
        mom_mult = 1.3
    elif h1_chg >= 5:
        mom_mult = 1.15
    elif h1_chg >= 3:
        mom_mult = 1.0

    # Volume multiplier — vol spike xác nhận thêm
    vol_mult = 1.0
    if h1_vol_ratio >= 4:
        vol_mult = 1.2
    elif h1_vol_ratio >= 2:
        vol_mult = 1.1

    # Combined — chỉ áp dụng cho TP2 và TP3
    ext_mult = mom_mult * vol_mult

    entry = coin.h1_close
    result.entry = round(entry, 8)

    def fmt(v: float) -> float:
        """Round đủ chữ số có nghĩa."""
        if v == 0:
            return 0.0
        import math
        digits = max(2, -int(math.floor(math.log10(abs(v)))) + 3)
        return round(v, digits)

    if result.reversal_type == "DUMP_REVERSAL":
        sl    = entry - h1_range * 0.3   # SL dưới đáy nến 1H 30% range → R:R tốt hơn
        tp1   = entry + h1_range * 0.5
        tp2   = entry + h1_range * 1.0 * ext_mult
        tp3   = entry + h1_range * 1.618 * ext_mult

        result.sl  = fmt(sl)
        result.tp1 = fmt(tp1)
        result.tp2 = fmt(tp2)
        result.tp3 = fmt(tp3)

        risk = entry - sl
        if risk > 0:
            result.rr_tp1 = round((tp1 - entry) / risk, 2)
            result.rr_tp2 = round((tp2 - entry) / risk, 2)

    else:  # PUMP_REVERSAL (Short)
        sl    = entry + h1_range * 0.3   # SL trên đỉnh nến 1H 30% range
        tp1   = entry - h1_range * 0.5
        tp2   = entry - h1_range * 1.0 * ext_mult
        tp3   = entry - h1_range * 1.618 * ext_mult

        result.sl  = fmt(sl)
        result.tp1 = fmt(tp1)
        result.tp2 = fmt(tp2)
        result.tp3 = fmt(tp3)

        risk = sl - entry
        if risk > 0:
            result.rr_tp1 = round((entry - tp1) / risk, 2)
            result.rr_tp2 = round((entry - tp2) / risk, 2)


def score_reversal(coin: CoinData) -> Optional[ScoreResult]:
    """
    1H Reversal Engine — phát hiện 2 loại:
    • PUMP_REVERSAL : coin pump mạnh trên 1D nhưng 1H đang đảo chiều xuống
    • DUMP_REVERSAL : coin dump mạnh trên 1D nhưng 1H đang bật ngược lên

    Điều kiện cần:
      - coin.h1_available = True
      - 1D thay đổi đủ mạnh (pump hoặc dump)
      - 1H hiện tại ngược chiều với 1D
    """
    if not coin.h1_available:
        return None
    if coin.h1_vol_ma10 <= 0:
        return None

    d1_chg  = coin.price_change_pct          # % change nến 1D hiện tại
    h1_chg  = coin.h1_price_change_pct       # % change nến 1H gần nhất
    h1_vol_ratio = coin.h1_volume / coin.h1_vol_ma10 if coin.h1_vol_ma10 > 0 else 0
    fr_pct  = coin.funding_rate * 100

    # Lookback 3 nến ngày: lấy pump/dump mạnh nhất trong 3 ngày gần nhất
    # → bắt được case MLN pump hôm qua, hôm nay đang đảo chiều
    d1_pump_max = max(d1_chg, coin.prev1d_change_pct, coin.prev2d_change_pct)
    d1_dump_max = min(d1_chg, coin.prev1d_change_pct, coin.prev2d_change_pct)

    # d1_ref: giá trị đại diện để hiển thị (nến nào pump/dump nhất)
    if d1_pump_max == coin.prev1d_change_pct:
        d1_pump_ref_label = "hôm qua"
    elif d1_pump_max == coin.prev2d_change_pct:
        d1_pump_ref_label = "hôm kia"
    else:
        d1_pump_ref_label = "hôm nay"

    if d1_dump_max == coin.prev1d_change_pct:
        d1_dump_ref_label = "hôm qua"
    elif d1_dump_max == coin.prev2d_change_pct:
        d1_dump_ref_label = "hôm kia"
    else:
        d1_dump_ref_label = "hôm nay"

    result  = ScoreResult(symbol=coin.symbol, exchange=coin.exchange)
    result.timeframe = "1H-REV"
    result.vol_ratio  = round(h1_vol_ratio, 2)
    result.oi_chg_pct = round(coin.oi_change_pct, 1)
    result.fr         = round(fr_pct, 4)
    result.lsr        = round(coin.lsr, 4)
    result.liq_ratio  = round(coin.liq_ratio, 2)
    result.price_current = round(coin.h1_close, 8)
    result.day_low       = round(coin.h1_low, 8)
    result.h1_chg        = round(h1_chg, 2)
    result.price_chg     = round(d1_chg, 2)   # nến 1D hiện tại để tham khảo

    details = []
    score   = 0.0

    # ── PUMP REVERSAL ─────────────────────────────────────────────
    # 1D pump mạnh (trong 3 ngày gần nhất) + 1H đang quay đầu xuống
    is_pump_rev = (
        d1_pump_max >= PUMP_REV_1D_MIN_PUMP
        and h1_chg <= -PUMP_REV_1H_DROP
    )

    # ── DUMP REVERSAL ─────────────────────────────────────────────
    # Điều kiện 1: 1D dump mạnh (lookback 3 ngày) + 1H bật ngược
    # Điều kiện 2 (MỚI): Intraday dump sâu trong nến ngày hiện tại + 1H bật ngược
    #   → bắt case như MLN: open→low dump 34% trong ngày, rồi 1H sau đó bật +10%
    is_dump_rev_lookback  = (
        d1_dump_max <= -DUMP_REV_1D_MIN_DUMP
        and h1_chg >= DUMP_REV_1H_PUMP
    )
    is_dump_rev_intraday = (
        coin.intraday_dump_pct >= INTRADAY_DUMP_MIN
        and h1_chg >= DUMP_REV_1H_PUMP
    )
    is_dump_rev = is_dump_rev_lookback or is_dump_rev_intraday

    if not is_pump_rev and not is_dump_rev:
        return None

    if is_pump_rev:
        result.reversal_type = "PUMP_REVERSAL"
        result.market_mode   = "PUMP_REVERSAL"
        details.append(f"🔄 Pump Reversal: 1D +{d1_pump_max:.1f}% ({d1_pump_ref_label}) → 1H {h1_chg:.1f}%")

        # Điểm theo độ mạnh của đảo chiều 1H
        if h1_chg <= -8:
            score += 3.0; details.append(f"📉 1H drop cực mạnh ({h1_chg:.1f}%)")
        elif h1_chg <= -5:
            score += 2.0; details.append(f"📉 1H drop mạnh ({h1_chg:.1f}%)")
        else:
            score += 1.0; details.append(f"📉 1H drop ({h1_chg:.1f}%)")

        # 1D pump càng cao → đà bán lại càng mạnh
        if d1_pump_max >= 30:
            score += 2.0; details.append(f"🚀 1D pump rất mạnh (+{d1_pump_max:.1f}%)")
        elif d1_pump_max >= 20:
            score += 1.5; details.append(f"🚀 1D pump mạnh (+{d1_pump_max:.1f}%)")
        elif d1_pump_max >= 10:
            score += 1.0; details.append(f"📈 1D pump (+{d1_pump_max:.1f}%)")

        # Vol 1H tăng khi đảo = xác nhận bán
        if h1_vol_ratio >= PUMP_REV_1H_VOL_MULT * 2:
            score += 2.0; details.append(f"💥 Vol 1H spike mạnh ({h1_vol_ratio:.1f}x)")
        elif h1_vol_ratio >= PUMP_REV_1H_VOL_MULT:
            score += 1.0; details.append(f"📊 Vol 1H tăng ({h1_vol_ratio:.1f}x)")

        # FR dương cao sau pump = long trap
        if fr_pct >= 0.10:
            score += 1.5; details.append(f"💥 FR dương cao ({fr_pct:.3f}%) — long trap")
        elif fr_pct >= 0.05:
            score += 0.5; details.append(f"⚠️ FR dương ({fr_pct:.4f}%)")
        # FR âm sâu sau pump = shorts vào quyết liệt, xác nhận đảo chiều
        elif fr_pct <= -0.20:
            score += 2.0; details.append(f"💥 FR âm sâu ({fr_pct:.3f}%) — shorts cực quyết")
        elif fr_pct <= -0.10:
            score += 1.0; details.append(f"⚠️ FR âm ({fr_pct:.4f}%) — shorts giữ mạnh")

        # OI giảm khi 1H đỏ = long đang thoát
        if coin.oi_change_pct <= -5:
            score += 1.5; details.append(f"📡 OI giảm ({coin.oi_change_pct:.1f}%) — long thoát")
        elif coin.oi_change_pct <= -2:
            score += 0.5; details.append(f"📡 OI giảm nhẹ ({coin.oi_change_pct:.1f}%)")

        # L/S crowded long = fuel thêm cho reversal
        if coin.lsr >= 2.7:
            score += 1.0; details.append(f"🐂 L/S crowded {coin.lsr:.3f}")

        # Nến 1H đỏ dài thân
        h1_body = abs(coin.h1_close - coin.h1_open)
        h1_upper = coin.h1_high - coin.h1_open
        if h1_body > 0 and h1_upper < h1_body * 0.3:
            score += 0.5; details.append("✅ Nến 1H đỏ thân dài")

        # Signal label
        if score >= 8:
            result.signal_type = "🔄💥 PUMP REV — BÁN RẤT MẠNH"
        elif score >= 6:
            result.signal_type = "🔄💥 PUMP REV — BÁN MẠNH"
        elif score >= 4:
            result.signal_type = "🔄📉 PUMP REVERSAL"
        else:
            result.signal_type = "🔄 Pump → Quay Đầu"

    else:  # DUMP_REVERSAL
        result.reversal_type = "DUMP_REVERSAL"
        result.market_mode   = "DUMP_REVERSAL"

        # Xác định nguồn gốc dump để hiển thị đúng
        if is_dump_rev_intraday and not is_dump_rev_lookback:
            # Intraday dump — dùng intraday_dump_pct làm đại diện
            effective_dump = -coin.intraday_dump_pct
            dump_source    = f"intraday open→low"
        else:
            effective_dump = d1_dump_max
            dump_source    = d1_dump_ref_label

        details.append(f"🔄 Dump Reversal: {effective_dump:.1f}% ({dump_source}) → 1H +{h1_chg:.1f}%")

        # Điểm theo độ mạnh bật ngược 1H
        if h1_chg >= 8:
            score += 3.0; details.append(f"🚀 1H bật cực mạnh (+{h1_chg:.1f}%)")
        elif h1_chg >= 5:
            score += 2.0; details.append(f"🚀 1H bật mạnh (+{h1_chg:.1f}%)")
        else:
            score += 1.0; details.append(f"📈 1H bật (+{h1_chg:.1f}%)")

        # Độ sâu dump — ưu tiên intraday nếu sâu hơn
        dump_depth = coin.intraday_dump_pct if coin.intraday_dump_pct >= abs(d1_dump_max) else abs(d1_dump_max)
        if dump_depth >= 30:
            score += 3.0; details.append(f"📉 Dump cực sâu (-{dump_depth:.1f}%) — nảy mạnh")
        elif dump_depth >= 20:
            score += 2.0; details.append(f"📉 Dump rất sâu (-{dump_depth:.1f}%)")
        elif dump_depth >= 12:
            score += 1.5; details.append(f"📉 Dump sâu (-{dump_depth:.1f}%)")
        elif dump_depth >= 8:
            score += 1.0; details.append(f"📉 Dump (-{dump_depth:.1f}%)")

        # Bonus thêm nếu là intraday dump — wick dài = "lau sàn" xong bật
        if is_dump_rev_intraday and coin.intraday_dump_pct >= INTRADAY_DUMP_MIN:
            lower_wick_pct = coin.intraday_dump_pct
            if lower_wick_pct >= 25:
                score += 1.5; details.append(f"🕯️ Wick dài cực mạnh (-{lower_wick_pct:.1f}%) — lau sàn xong bật")
            elif lower_wick_pct >= 15:
                score += 1.0; details.append(f"🕯️ Wick dài (-{lower_wick_pct:.1f}%) — tín hiệu đáy tạm")

        # Vol 1H tăng khi bật = xác nhận mua vào
        if h1_vol_ratio >= DUMP_REV_1H_VOL_MULT * 2:
            score += 2.0; details.append(f"💥 Vol 1H spike mạnh ({h1_vol_ratio:.1f}x)")
        elif h1_vol_ratio >= DUMP_REV_1H_VOL_MULT:
            score += 1.0; details.append(f"📊 Vol 1H tăng ({h1_vol_ratio:.1f}x)")

        # FR âm sâu sau dump = short squeeze tiềm năng
        if fr_pct <= -0.10:
            score += 1.5; details.append(f"💥 FR âm sâu ({fr_pct:.3f}%) — short squeeze")
        elif fr_pct <= -0.05:
            score += 0.5; details.append(f"💰 FR âm ({fr_pct:.4f}%)")

        # OI tăng khi 1H xanh = short mới vào = short squeeze fuel
        if coin.oi_change_pct >= 10:
            score += 1.5; details.append(f"📡 OI tăng ({coin.oi_change_pct:.1f}%) — short squeeze setup")
        elif coin.oi_change_pct >= 5:
            score += 0.5; details.append(f"📡 OI tăng nhẹ ({coin.oi_change_pct:.1f}%)")

        # Liq: longs bị clear trong dump → sạch nhiên liệu để bật
        if coin.liq_longs > 0:
            longs_x = coin.liq_longs / max(coin.liq_shorts, 1)
            if longs_x >= 3:
                score += 1.0; details.append(f"🧹 Long đã bị liq sạch ({longs_x:.0f}x)")

        # Nến 1H xanh dài thân
        h1_body = abs(coin.h1_close - coin.h1_open)
        h1_lower = coin.h1_open - coin.h1_low
        if coin.h1_close > coin.h1_open and h1_body > 0 and h1_lower < h1_body * 0.3:
            score += 0.5; details.append("✅ Nến 1H xanh thân dài")

        # Signal label
        if score >= 8:
            result.signal_type = "🔄💥 DUMP REV — MUA RẤT MẠNH"
        elif score >= 6:
            result.signal_type = "🔄💥 DUMP REV — MUA MẠNH"
        elif score >= 4:
            result.signal_type = "🔄📈 DUMP REVERSAL"
        else:
            result.signal_type = "🔄 Dump → Bật Ngược"

    result.total_score = round(score, 1)
    result.details     = details

    if result.total_score < MIN_REVERSAL_SCORE:
        return None

    # ── M30 Confirmation ─────────────────────────────────────────
    # Dùng nến M30 để xác nhận / cập nhật tín hiệu H1
    # M30 cùng chiều = tín hiệu mạnh hơn, ngược chiều = cảnh báo
    if coin.m30_available:
        m30_chg      = coin.m30_price_change_pct
        m30_prev_chg = coin.m30_prev_change_pct
        m30_vol_ratio = coin.m30_volume / coin.m30_vol_ma10 if coin.m30_vol_ma10 > 0 else 0

        result.m30_chg = round(m30_chg, 2)

        if result.reversal_type == "DUMP_REVERSAL":
            # M30 xanh = xác nhận bật ngược
            if m30_chg > 0 and m30_prev_chg > 0:
                # 2 nến M30 liên tiếp xanh = momentum đang hình thành
                result.total_score += 1.5
                result.m30_confirmed = True
                result.details.append(f"✅ M30 xác nhận: 2 nến xanh ({m30_prev_chg:+.1f}% → {m30_chg:+.1f}%)")
            elif m30_chg > 0:
                result.total_score += 0.8
                result.m30_confirmed = True
                result.details.append(f"✅ M30 xác nhận: nến xanh ({m30_chg:+.1f}%)")
            elif m30_chg < -2:
                # M30 đỏ ngược chiều = cảnh báo, trừ điểm
                result.total_score -= 1.0
                result.details.append(f"⚠️ M30 ngược chiều ({m30_chg:.1f}%) — chờ xác nhận")
            else:
                result.details.append(f"➡️ M30 sideway ({m30_chg:+.1f}%)")

            # M30 vol spike = xác nhận mua vào thực sự
            if m30_vol_ratio >= 3:
                result.total_score += 1.0
                result.details.append(f"💥 M30 vol spike ({m30_vol_ratio:.1f}x)")
            elif m30_vol_ratio >= 1.5:
                result.total_score += 0.5
                result.details.append(f"📊 M30 vol tăng ({m30_vol_ratio:.1f}x)")

        else:  # PUMP_REVERSAL
            # M30 đỏ = xác nhận đảo chiều xuống
            if m30_chg < 0 and m30_prev_chg < 0:
                result.total_score += 1.5
                result.m30_confirmed = True
                result.details.append(f"✅ M30 xác nhận: 2 nến đỏ ({m30_prev_chg:+.1f}% → {m30_chg:+.1f}%)")
            elif m30_chg < 0:
                result.total_score += 0.8
                result.m30_confirmed = True
                result.details.append(f"✅ M30 xác nhận: nến đỏ ({m30_chg:+.1f}%)")
            elif m30_chg > 2:
                result.total_score -= 1.0
                result.details.append(f"⚠️ M30 ngược chiều ({m30_chg:+.1f}%) — chờ xác nhận")
            else:
                result.details.append(f"➡️ M30 sideway ({m30_chg:+.1f}%)")

            if m30_vol_ratio >= 3:
                result.total_score += 1.0
                result.details.append(f"💥 M30 vol spike ({m30_vol_ratio:.1f}x)")
            elif m30_vol_ratio >= 1.5:
                result.total_score += 0.5
                result.details.append(f"📊 M30 vol tăng ({m30_vol_ratio:.1f}x)")

        result.total_score = round(result.total_score, 1)

    # Tính số phút còn lại đến khi nến H1 đóng
    now_utc = datetime.now(timezone.utc)
    mins_into_hour = now_utc.minute + now_utc.second / 60
    result.h1_minutes_left = max(0, int(60 - mins_into_hour))

    # Tính TP/SL dựa trên momentum 1H
    calc_reversal_tp(result, coin)

    return result


# ══════════════════════════════════════════════════════════════
# SCANNER
# ══════════════════════════════════════════════════════════════

def score_h1_breakout(coin: CoinData) -> Optional[ScoreResult]:
    """
    1H Momentum Breakout — signal độc lập với 1D.

    Điều kiện cần:
      • h1_available = True
      • |h1_chg| ≥ H1_BREAKOUT_MIN_CHG (8%)
      • h1_vol_ratio ≥ H1_BREAKOUT_MIN_VOL (5x)

    2 chiều:
      • H1_BREAKOUT_LONG  : h1_chg ≥ +8%, vol ≥ 5x → pump mạnh, có thể long
      • H1_BREAKOUT_SHORT : h1_chg ≤ -8%, vol ≥ 5x → dump mạnh, có thể short

    Bổ sung điểm từ:
      • FR âm sâu (short squeeze fuel cho long)
      • FR dương cao (long trap → xác nhận short)
      • OI tăng cùng chiều
      • M30 xác nhận
      • Liq cùng chiều
    """
    if not coin.h1_available:
        return None
    if coin.h1_vol_ma10 <= 0:
        return None

    h1_chg       = coin.h1_price_change_pct
    h1_vol_ratio = coin.h1_volume / coin.h1_vol_ma10
    fr_pct       = coin.funding_rate * 100
    abs_chg      = abs(h1_chg)

    # Filter cơ bản
    if abs_chg < H1_BREAKOUT_MIN_CHG:
        return None
    if h1_vol_ratio < H1_BREAKOUT_MIN_VOL:
        return None

    is_long  = h1_chg > 0
    is_short = h1_chg < 0

    result = ScoreResult(symbol=coin.symbol, exchange=coin.exchange)
    result.timeframe    = "1H-BO"
    result.vol_ratio    = round(h1_vol_ratio, 2)
    result.oi_chg_pct   = round(coin.oi_change_pct, 1)
    result.fr           = round(fr_pct, 4)
    result.lsr          = round(coin.lsr, 4)
    result.liq_ratio    = round(coin.liq_ratio, 2)
    result.price_current = round(coin.h1_close, 8)
    result.day_low       = round(coin.h1_low, 8)
    result.h1_chg        = round(h1_chg, 2)
    result.price_chg     = round(coin.price_change_pct, 2)

    score   = 0.0
    details = []

    if is_long:
        result.reversal_type = "H1_BREAKOUT_LONG"
        details.append(f"🚀 H1 Breakout Long: +{h1_chg:.1f}% vol {h1_vol_ratio:.1f}x")

        # Momentum 1H
        if abs_chg >= 20:
            score += 3.0; details.append(f"🔥 H1 cực mạnh (+{abs_chg:.1f}%)")
        elif abs_chg >= 12:
            score += 2.5; details.append(f"💪 H1 rất mạnh (+{abs_chg:.1f}%)")
        elif abs_chg >= 8:
            score += 2.0; details.append(f"📈 H1 mạnh (+{abs_chg:.1f}%)")

        # Vol spike
        if h1_vol_ratio >= 10:
            score += 3.0; details.append(f"💥 Vol spike cực mạnh ({h1_vol_ratio:.1f}x)")
        elif h1_vol_ratio >= 7:
            score += 2.5; details.append(f"💥 Vol spike rất mạnh ({h1_vol_ratio:.1f}x)")
        elif h1_vol_ratio >= 5:
            score += 2.0; details.append(f"📊 Vol spike ({h1_vol_ratio:.1f}x)")

        # FR âm → shorts đang trả phí → squeeze fuel → cộng điểm
        if fr_pct <= -0.5:
            score += 3.0; details.append(f"🔴 FR âm cực sâu ({fr_pct:.3f}%) — short squeeze")
        elif fr_pct <= -0.2:
            score += 2.0; details.append(f"💥 FR âm sâu ({fr_pct:.3f}%) — squeeze fuel")
        elif fr_pct <= H1_BREAKOUT_FR_BONUS:
            score += 1.0; details.append(f"💰 FR âm ({fr_pct:.4f}%)")

        # OI tăng khi giá tăng = long mới vào = momentum thật
        if coin.oi_change_pct >= 15:
            score += 1.5; details.append(f"📡 OI tăng mạnh (+{coin.oi_change_pct:.1f}%) — long vào")
        elif coin.oi_change_pct >= 5:
            score += 0.5; details.append(f"📡 OI tăng ({coin.oi_change_pct:.1f}%)")
        # OI giảm khi giá tăng = short đang cover = cũng tốt
        elif coin.oi_change_pct <= -5:
            score += 1.0; details.append(f"📡 OI giảm ({coin.oi_change_pct:.1f}%) — short cover")

        # Liq: shorts bị liq = fuel
        if coin.liq_ratio >= 3:
            score += 1.5; details.append(f"💥 Shorts liq {coin.liq_ratio:.1f}x")
        elif coin.liq_ratio >= 1.5:
            score += 0.5; details.append(f"✅ Shorts liq {coin.liq_ratio:.1f}x")

        # Nến 1H xanh đẹp (thân dài, bóng ngắn)
        h1_body  = abs(coin.h1_close - coin.h1_open)
        h1_upper = coin.h1_high - coin.h1_close
        if h1_body > 0 and h1_upper < h1_body * 0.3:
            score += 0.5; details.append("✅ Nến 1H xanh thân dài")

        # Signal label
        if score >= 10:
            result.signal_type = "🚀💥 H1 BREAKOUT LONG — CỰC MẠNH"
        elif score >= 7:
            result.signal_type = "🚀 H1 BREAKOUT LONG — RẤT MẠNH"
        elif score >= 5:
            result.signal_type = "📈 H1 BREAKOUT LONG"
        else:
            result.signal_type = "⚡ H1 Long Signal"

    else:  # is_short
        result.reversal_type = "H1_BREAKOUT_SHORT"
        details.append(f"📉 H1 Breakout Short: {h1_chg:.1f}% vol {h1_vol_ratio:.1f}x")

        # Momentum 1H dump
        if abs_chg >= 20:
            score += 3.0; details.append(f"🔥 H1 dump cực mạnh ({h1_chg:.1f}%)")
        elif abs_chg >= 12:
            score += 2.5; details.append(f"💪 H1 dump rất mạnh ({h1_chg:.1f}%)")
        elif abs_chg >= 8:
            score += 2.0; details.append(f"📉 H1 dump mạnh ({h1_chg:.1f}%)")

        # Vol spike
        if h1_vol_ratio >= 10:
            score += 3.0; details.append(f"💥 Vol spike cực mạnh ({h1_vol_ratio:.1f}x)")
        elif h1_vol_ratio >= 7:
            score += 2.5; details.append(f"💥 Vol spike rất mạnh ({h1_vol_ratio:.1f}x)")
        elif h1_vol_ratio >= 5:
            score += 2.0; details.append(f"📊 Vol spike ({h1_vol_ratio:.1f}x)")

        # FR dương cao = long trap = dump fuel
        if fr_pct >= 0.2:
            score += 2.0; details.append(f"💥 FR dương cao ({fr_pct:.3f}%) — long trap")
        elif fr_pct >= 0.05:
            score += 1.0; details.append(f"⚠️ FR dương ({fr_pct:.4f}%)")
        # FR âm khi dump = shorts không tin tưởng, dump có thể đảo
        elif fr_pct <= -0.2:
            score -= 0.5; details.append(f"⚠️ FR âm ({fr_pct:.3f}%) — dump yếu hơn")

        # OI tăng khi giá giảm = short mới vào = dump tiếp
        if coin.oi_change_pct >= 15:
            score += 1.5; details.append(f"📡 OI tăng ({coin.oi_change_pct:.1f}%) — short vào")
        elif coin.oi_change_pct >= 5:
            score += 0.5; details.append(f"📡 OI tăng nhẹ ({coin.oi_change_pct:.1f}%)")

        # Liq: longs bị liq = cascade
        if coin.liq_longs > 0:
            lx = coin.liq_longs / max(coin.liq_shorts, 1)
            if lx >= 5:
                score += 2.0; details.append(f"💥 Longs liq {lx:.0f}x — cascade")
            elif lx >= 2:
                score += 1.0; details.append(f"✅ Longs liq {lx:.1f}x")

        # Nến 1H đỏ thân dài
        h1_body  = abs(coin.h1_close - coin.h1_open)
        h1_upper = coin.h1_high - coin.h1_open
        if h1_body > 0 and h1_upper < h1_body * 0.3:
            score += 0.5; details.append("✅ Nến 1H đỏ thân dài")

        # Signal label
        if score >= 10:
            result.signal_type = "📉💥 H1 BREAKOUT SHORT — CỰC MẠNH"
        elif score >= 7:
            result.signal_type = "📉 H1 BREAKOUT SHORT — RẤT MẠNH"
        elif score >= 5:
            result.signal_type = "⬇️ H1 BREAKOUT SHORT"
        else:
            result.signal_type = "⚡ H1 Short Signal"

    # M30 xác nhận (giống reversal engine)
    if coin.m30_available and coin.m30_vol_ma10 > 0:
        m30_chg       = coin.m30_price_change_pct
        m30_vol_ratio = coin.m30_volume / coin.m30_vol_ma10
        result.m30_chg = round(m30_chg, 2)

        if is_long:
            if m30_chg > 0:
                score += 1.0; result.m30_confirmed = True
                details.append(f"✅ M30 xác nhận ({m30_chg:+.1f}%)")
            elif m30_chg < -2:
                score -= 0.5
                details.append(f"⚠️ M30 ngược ({m30_chg:.1f}%)")
        else:
            if m30_chg < 0:
                score += 1.0; result.m30_confirmed = True
                details.append(f"✅ M30 xác nhận ({m30_chg:+.1f}%)")
            elif m30_chg > 2:
                score -= 0.5
                details.append(f"⚠️ M30 ngược ({m30_chg:+.1f}%)")

        if m30_vol_ratio >= 3:
            score += 0.5; details.append(f"💥 M30 vol ({m30_vol_ratio:.1f}x)")

    result.total_score = round(score, 1)
    result.details     = details

    # Số phút còn lại đến khi H1 đóng
    now_utc = datetime.now(timezone.utc)
    mins_into = now_utc.minute + now_utc.second / 60
    result.h1_minutes_left = max(0, int(60 - mins_into))

    if result.total_score < H1_BREAKOUT_MIN_SCORE:
        return None

    # Tính TP/SL — dùng lại calc_reversal_tp với map reversal_type
    if is_long:
        result.reversal_type = "DUMP_REVERSAL"   # map tạm để dùng LONG formula
        calc_reversal_tp(result, coin)
        result.reversal_type = "H1_BREAKOUT_LONG"
    else:
        result.reversal_type = "PUMP_REVERSAL"   # map tạm để dùng SHORT formula
        calc_reversal_tp(result, coin)
        result.reversal_type = "H1_BREAKOUT_SHORT"

    return result


def calc_h2_tp_sl(result: ScoreResult, h2_high: float, h2_low: float,
                   h2_close: float, direction: str) -> None:
    """
    TP/SL ngắn hạn cho H2 — target 15-30%.
    TP1 = range*0.5, TP2 = range*1.0, TP3 = range*1.5
    """
    import math
    h2_range = h2_high - h2_low
    if h2_range <= 0 or h2_close <= 0:
        return

    def fmt(v: float) -> float:
        if v <= 0: return 0.0
        digits = max(2, -int(math.floor(math.log10(abs(v)))) + 3)
        return round(v, digits)

    entry = h2_close
    result.entry = fmt(entry)

    if direction == "PUMP":
        result.sl  = fmt(h2_low  - h2_range * 0.15)   # SL chặt hơn D
        result.tp1 = fmt(entry   + h2_range * 0.5)     # ~10-15%
        result.tp2 = fmt(entry   + h2_range * 1.0)     # ~20-30%
        result.tp3 = fmt(entry   + h2_range * 1.5)     # ~35-45%
    else:  # DUMP
        result.sl  = fmt(h2_high + h2_range * 0.15)
        result.tp1 = fmt(entry   - h2_range * 0.5)
        result.tp2 = fmt(entry   - h2_range * 1.0)
        result.tp3 = fmt(entry   - h2_range * 1.5)

    risk = abs(entry - result.sl)
    reward1 = abs(result.tp1 - entry)
    reward2 = abs(result.tp2 - entry)
    if risk > 0:
        result.rr_tp1 = round(reward1 / risk, 1)
        result.rr_tp2 = round(reward2 / risk, 1)


def score_coin_h2(exchange: str, symbol: str) -> Optional[ScoreResult]:
    """
    Score coin khung H2 — tìm pump/dump ngắn hạn sau khi nến H2 đóng.
    Ngưỡng thấp hơn 1D: MIN_CHG=7%, MIN_VOL=1.3x, MIN_SCORE=4.0đ
    Target: TP 15-30%

    Điểm đặc biệt so với D:
    - Thêm bonus nến không bóng dưới (low=open) → mua mạnh
    - Thêm bonus FR âm + pump = short squeeze H2
    - Liq shorts >> longs = squeeze setup
    """
    candles = get_ohlcv_h2(exchange, symbol, limit=15)
    if not candles or len(candles) < 5:
        return None

    # Nến vừa đóng = candles[-1]
    latest = candles[-1]
    h2_o = float(latest.get("o", 0)); h2_h = float(latest.get("h", 0))
    h2_l = float(latest.get("l", 0)); h2_c = float(latest.get("c", 0))
    h2_v = float(latest.get("v", 0))
    if h2_o <= 0 or h2_c <= 0:
        return None

    # Vol MA10 từ 10 nến trước
    prev_vols = [float(c.get("v", 0)) for c in candles[-11:-1]]
    vol_ma = sum(prev_vols) / len(prev_vols) if prev_vols else 0
    if vol_ma <= 0:
        return None

    h2_chg = (h2_c - h2_o) / h2_o * 100
    vol_ratio = h2_v / vol_ma

    # Filter cơ bản
    if abs(h2_chg) < H2_MIN_CHG:
        return None

    direction = "PUMP" if h2_chg > 0 else "DUMP"

    # Lấy thêm data FR, OI, LSR, Liq
    fr  = get_funding_rate(exchange, symbol) or 0
    oi_hist = get_oi_history(exchange, symbol, limit=4)
    oi_chg = 0.0
    if oi_hist and len(oi_hist) >= 2:
        oi_new = float(oi_hist[-1].get("openInterest", 0))
        oi_old = float(oi_hist[-3].get("openInterest", oi_new))
        if oi_old > 0:
            oi_chg = (oi_new - oi_old) / oi_old * 100

    lsr = get_lsr(exchange, symbol) or 1.0
    liq_longs, liq_shorts = get_liquidation(exchange, symbol)
    fr_pct = fr * 100

    result = ScoreResult(symbol=symbol, exchange=exchange)
    result.timeframe     = "2H"
    result.price_current = round(h2_c, 8)
    result.day_low       = round(h2_l, 8)
    result.price_chg     = round(h2_chg, 2)
    result.vol_ratio     = round(vol_ratio, 2)
    result.oi_chg_pct    = round(oi_chg, 1)
    result.fr            = round(fr_pct, 4)
    result.lsr           = round(lsr, 4)
    result.liq_ratio     = round(liq_shorts / liq_longs, 2) if liq_longs > 0 else 0

    score   = 0.0
    details = []

    if direction == "PUMP":
        # 1. Momentum H2
        if abs(h2_chg) >= 20 and vol_ratio >= 2:
            score += 3.0; details.append(f"🚀 H2 pump cực mạnh (+{h2_chg:.1f}% vol {vol_ratio:.1f}x)")
        elif abs(h2_chg) >= 12 and vol_ratio >= 1.5:
            score += 2.5; details.append(f"🚀 H2 pump mạnh (+{h2_chg:.1f}% vol {vol_ratio:.1f}x)")
        elif abs(h2_chg) >= 20:
            score += 2.5; details.append(f"🚀 H2 thin-air cực (+{h2_chg:.1f}%)")
        elif abs(h2_chg) >= 12:
            score += 2.0; details.append(f"🚀 H2 thin-air (+{h2_chg:.1f}%)")
        else:
            score += 1.5; details.append(f"📈 H2 pump (+{h2_chg:.1f}%)")

        # 2. Vol spike
        if vol_ratio >= 5:
            score += 3.0; details.append(f"💥 Vol H2 spike cực mạnh ({vol_ratio:.1f}x)")
        elif vol_ratio >= 3:
            score += 2.0; details.append(f"💥 Vol H2 spike ({vol_ratio:.1f}x)")
        elif vol_ratio >= H2_MIN_VOL:
            score += 1.0; details.append(f"📊 Vol H2 tăng ({vol_ratio:.1f}x)")

        # 3. OI
        if oi_chg >= 20 and oi_chg > abs(h2_chg):
            score += 2.0; details.append(f"📡 OI +{oi_chg:.1f}% — long mới vào mạnh")
        elif oi_chg >= 10:
            score += 1.0; details.append(f"📡 OI +{oi_chg:.1f}%")
        elif oi_chg <= -5:
            score += 1.0; details.append(f"📡 OI -{abs(oi_chg):.1f}% — short cover")

        # 4. FR âm = short squeeze fuel
        if fr_pct <= -0.3:
            score += 2.0; details.append(f"💥 FR âm sâu ({fr_pct:.3f}%) — short squeeze")
        elif fr_pct <= -0.05:
            score += 1.0; details.append(f"💰 FR âm ({fr_pct:.4f}%)")
        elif fr_pct <= 0.05:
            score += 0.5; details.append(f"💰 FR thấp ({fr_pct:.4f}%)")

        # 5. Liq: shorts bị liq >> longs
        if liq_longs > 0:
            liq_ratio = liq_shorts / liq_longs
            if liq_ratio >= 5:
                score += 2.0; details.append(f"💥 Shorts liq {liq_ratio:.0f}x — squeeze mạnh")
            elif liq_ratio >= 2:
                score += 1.0; details.append(f"✅ Shorts liq {liq_ratio:.1f}x")
        elif liq_shorts > 0:
            score += 1.5; details.append(f"💥 Shorts liq — không có long bị liq")

        # 6. Nến đặc biệt: low=open = không có selling pressure
        if abs(h2_l - h2_o) / h2_o < 0.002:   # low ≈ open (trong 0.2%)
            score += 1.0; details.append("🕯️ Low≈Open — không có bóng dưới (mua mạnh)")

        # 7. LSR
        if 1.0 <= lsr <= 2.3:
            score += 0.5; details.append(f"📈 L/S {lsr:.3f} ✅")
        elif lsr > 2.3:
            score -= 0.5

        result.signal_type = (
            "🚀💥 H2 SHORT SQUEEZE" if fr_pct <= -0.1 and (liq_shorts > liq_longs * 2) else
            "🚀 H2 PUMP MẠNH" if abs(h2_chg) >= 15 else
            "📈 H2 PUMP"
        )

    else:  # DUMP
        if abs(h2_chg) >= 20 and vol_ratio >= 2:
            score += 3.0; details.append(f"📉 H2 dump cực mạnh ({h2_chg:.1f}% vol {vol_ratio:.1f}x)")
        elif abs(h2_chg) >= 12:
            score += 2.0; details.append(f"📉 H2 dump ({h2_chg:.1f}%)")
        else:
            score += 1.5; details.append(f"🔻 H2 giảm ({h2_chg:.1f}%)")

        if vol_ratio >= 5:
            score += 3.0; details.append(f"💥 Panic vol ({vol_ratio:.1f}x)")
        elif vol_ratio >= 3:
            score += 2.0
        elif vol_ratio >= H2_MIN_VOL:
            score += 1.0

        if oi_chg >= 10:
            score += 1.0; details.append(f"📡 OI +{oi_chg:.1f}% — short vào")
        elif oi_chg <= -10:
            score += 1.5; details.append(f"📡 OI -{abs(oi_chg):.1f}% — long tháo")

        if fr_pct >= 0.15:
            score += 1.5; details.append(f"💥 FR dương cao ({fr_pct:.3f}%) — long trap")
        elif fr_pct >= 0.05:
            score += 0.5

        if liq_longs > 0 and liq_shorts > 0:
            lr = liq_longs / liq_shorts
            if lr >= 5:
                score += 2.0; details.append(f"💥 Longs liq {lr:.0f}x — cascade")
            elif lr >= 2:
                score += 1.0

        result.signal_type = "📉 H2 DUMP MẠNH" if abs(h2_chg) >= 15 else "⬇️ H2 DUMP"

    result.total_score = round(score, 1)
    result.details     = details
    result.market_mode = f"H2_{direction}"

    if result.total_score < H2_MIN_SCORE:
        return None

    calc_h2_tp_sl(result, h2_h, h2_l, h2_c, direction)
    return result


def scan_one_symbol(exchange: str, symbol: str) -> tuple[
    Optional[ScoreResult], Optional[ScoreResult], Optional[ScoreResult]
]:
    """Scan 1 coin. Trả về (pump_result, dump_result, reversal_result)."""
    coin = fetch_coin_data(exchange, symbol)
    if coin is None:
        return None, None, None
    pump = score_coin_pump(coin)
    dump = score_coin_dump(coin)
    # Lấy signal tốt nhất: reversal hoặc h1_breakout
    rev1 = score_reversal(coin)
    rev2 = score_h1_breakout(coin)
    if rev1 and rev2:
        reversal = rev1 if rev1.total_score >= rev2.total_score else rev2
    else:
        reversal = rev1 or rev2
    return pump, dump, reversal


def run_scan_exchange(exchange: str) -> tuple[list[ScoreResult], list[ScoreResult], list[ScoreResult]]:
    log.info("=" * 60)
    log.info(f"🔍 FAST SCAN {exchange} — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    log.info("=" * 60)

    symbols = get_all_symbols(exchange)
    if not symbols:
        log.error(f"Không lấy được danh sách symbols {exchange}!")
        return [], [], []

    pump_results: list[ScoreResult] = []
    dump_results: list[ScoreResult] = []
    rev_results:  list[ScoreResult] = []
    errors = 0
    workers = MAX_WORKERS_BINANCE if exchange == "Binance" else MAX_WORKERS_BINGX if exchange == "BingX" else MAX_WORKERS_KUCOIN if exchange == "KuCoin" else MAX_WORKERS_BYBIT

    log.info(f"🚀 {exchange}: scanning {len(symbols)} symbols với {workers} workers...")

    if not FAST_SCAN:
        for i, symbol in enumerate(symbols, 1):
            try:
                if i == 1 or i % LOG_EVERY_N == 0 or i == len(symbols):
                    log.info(f"[{exchange} {i}/{len(symbols)}] Scanning...")
                pump, dump, rev = scan_one_symbol(exchange, symbol)
                if pump: pump_results.append(pump)
                if dump: dump_results.append(dump)
                if rev:  rev_results.append(rev)
            except Exception as e:
                errors += 1
                log.warning(f"  ❌ {exchange} {symbol}: {e}")
        pump_results.sort(key=lambda x: x.total_score, reverse=True)
        dump_results.sort(key=lambda x: x.total_score, reverse=True)
        rev_results.sort(key=lambda x: x.total_score, reverse=True)
        log.info(f"✅ {exchange} scan xong | pump {len(pump_results)} | dump {len(dump_results)} | rev {len(rev_results)} | errors {errors}")
        return pump_results, dump_results, rev_results

    completed = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {executor.submit(scan_one_symbol, exchange, s): s for s in symbols}

        for future in as_completed(future_map):
            symbol = future_map[future]
            completed += 1
            try:
                pump, dump, rev = future.result()
                if pump:
                    pump_results.append(pump)
                    log.info(f"  ✅ PUMP {pump.display_symbol}: {pump.total_score:.1f}đ — {pump.signal_type}")
                if dump:
                    dump_results.append(dump)
                    log.info(f"  ✅ DUMP {dump.display_symbol}: {dump.total_score:.1f}đ — {dump.signal_type}")
                if rev:
                    rev_results.append(rev)
                    log.info(f"  🔄 REV  {rev.display_symbol}: {rev.total_score:.1f}đ — {rev.signal_type}")
            except Exception as e:
                errors += 1
                log.debug(f"  ❌ {exchange} {symbol}: {e}")

            if completed == 1 or completed % LOG_EVERY_N == 0 or completed == len(symbols):
                log.info(f"[{exchange}] Progress {completed}/{len(symbols)} | pump {len(pump_results)} | dump {len(dump_results)} | rev {len(rev_results)} | errors {errors}")

    pump_results.sort(key=lambda x: x.total_score, reverse=True)
    dump_results.sort(key=lambda x: x.total_score, reverse=True)
    rev_results.sort(key=lambda x: x.total_score, reverse=True)
    log.info(f"✅ {exchange} FAST scan xong: {len(symbols)} symbols | pump {len(pump_results)} | dump {len(dump_results)} | rev {len(rev_results)} | errors {errors}")
    return pump_results, dump_results, rev_results

def run_scan() -> tuple[list[ScoreResult], list[ScoreResult], list[ScoreResult]]:
    """
    Quét cả 4 sàn SONG SONG, gộp kết quả, trả về (pump_top, dump_top, reversal_top).

    Kiến trúc parallel 2 tầng:
      Tầng 1: 4 sàn chạy đồng thời (ThreadPoolExecutor MAX_WORKERS_EXCHANGES=4)
      Tầng 2: Mỗi sàn scan symbol của mình song song (workers riêng từng sàn)

    Quy tắc PUMP: SQUEEZE ưu tiên TOP 1, còn lại theo total_score.
    Quy tắc DUMP: top 2 theo total_score.
    Quy tắc REVERSAL: top 1 mỗi loại (PUMP_REV + DUMP_REV).
    """
    TOP_PUMP = 1
    TOP_DUMP = 1

    all_pump: list[ScoreResult] = []
    all_dump: list[ScoreResult] = []
    all_rev:  list[ScoreResult] = []

    scan_start = time.time()
    log.info(f"🚀 Parallel scan bắt đầu: {len(SCAN_EXCHANGES)} sàn đồng thời...")

    # Tầng 1: 4 sàn chạy song song
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_EXCHANGES) as ex_pool:
        future_map = {
            ex_pool.submit(run_scan_exchange, exchange): exchange
            for exchange in SCAN_EXCHANGES
        }
        for future in as_completed(future_map):
            exchange = future_map[future]
            try:
                pump_ex, dump_ex, rev_ex = future.result()
                if PER_EXCHANGE_TOP_N:
                    all_pump.extend(pump_ex[:TOP_N_FINAL])
                    all_dump.extend(dump_ex[:TOP_N_FINAL])
                    all_rev.extend(rev_ex[:TOP_N_FINAL])
                else:
                    all_pump.extend(pump_ex)
                    all_dump.extend(dump_ex)
                    all_rev.extend(rev_ex)
                log.info(
                    f"✅ {exchange} xong: "
                    f"pump {len(pump_ex)} | dump {len(dump_ex)} | rev {len(rev_ex)} "
                    f"| elapsed {time.time()-scan_start:.0f}s"
                )
            except Exception as e:
                log.error(f"❌ {exchange} scan error: {e}", exc_info=True)

    log.info(f"⏱️ Parallel scan hoàn tất trong {time.time()-scan_start:.1f}s")
    log.info(f"   Tổng trước dedup: pump {len(all_pump)} | dump {len(all_dump)} | rev {len(all_rev)}")

    def dedup(lst: list[ScoreResult]) -> list[ScoreResult]:
        """Cùng symbol giữ bản có điểm cao nhất."""
        seen: dict[str, ScoreResult] = {}
        for r in sorted(lst, key=lambda x: x.total_score, reverse=True):
            base = r.symbol.upper()
            # Với KuCoin symbol BTCUSDTM, strip M để dedup với BTCUSDT của sàn khác
            if base.endswith("USDTM"):
                base = base[:-1]  # BTCUSDTM → BTCUSDT
            if base not in seen:
                seen[base] = r
        return list(seen.values())

    unique_pump = dedup(all_pump)
    unique_dump = dedup(all_dump)
    unique_rev  = dedup(all_rev)

    # ── PUMP: SQUEEZE ưu tiên TOP 1 ──────────────────────────────
    squeezes = [
        r for r in unique_pump
        if r.market_mode in ("SQUEEZE", "HYBRID") or r.squeeze_engine_score >= SQUEEZE_MIN_SCORE
    ]
    squeezes.sort(key=lambda x: (x.squeeze_engine_score, x.total_score), reverse=True)

    final_pump: list[ScoreResult] = []
    if squeezes:
        top_squeeze = squeezes[0]
        final_pump.append(top_squeeze)
        remaining = [r for r in unique_pump if r.symbol.upper().rstrip("M") != top_squeeze.symbol.upper().rstrip("M")]
    else:
        remaining = unique_pump
    remaining.sort(key=lambda x: x.total_score, reverse=True)
    final_pump.extend(remaining)
    final_pump = final_pump[:TOP_PUMP]

    # ── DUMP: theo total_score ─────────────────────────────────────
    unique_dump.sort(key=lambda x: x.total_score, reverse=True)
    final_dump = unique_dump[:TOP_DUMP]

    # ── REVERSAL: top 1 mỗi loại ──────────────────────────────────
    # ── REVERSAL: top 1 mỗi loại signal ─────────────────────────
    def top1_by_type(rtype: str) -> Optional[ScoreResult]:
        filtered = [r for r in unique_rev if r.reversal_type == rtype]
        return max(filtered, key=lambda x: x.total_score) if filtered else None

    final_rev: list[ScoreResult] = []
    for rtype in ("PUMP_REVERSAL", "DUMP_REVERSAL", "H1_BREAKOUT_LONG", "H1_BREAKOUT_SHORT"):
        r = top1_by_type(rtype)
        if r:
            final_rev.append(r)

    return final_pump, final_dump, final_rev


# ══════════════════════════════════════════════════════════════
# TELEGRAM ALERT
# ══════════════════════════════════════════════════════════════

def send_telegram(message: str) -> bool:
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "HTML",
        "disable_web_page_preview": True,
    }
    try:
        r = get_session().post(url, json=payload, timeout=10)
        if r.status_code != 200:
            log.error(f"Telegram failed {r.status_code}: {r.text[:500]}")
            return False
        return True
    except Exception as e:
        log.error(f"Telegram error: {e}")
        return False


def format_alert(pump_results: list[ScoreResult], dump_results: list[ScoreResult],
                 rev_results: list[ScoreResult]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"🚀📉 <b>PUMP &amp; DUMP SCANNER V6</b>",
        f"🕒 <b>{now}</b>",
        f"📊 Quét: {' | '.join(SCAN_EXCHANGES)} — 1D + 1H\n",
    ]

    # ── PUMP SECTION ──────────────────────────────────────────────
    lines.append("═══════════════════════════")
    lines.append("🚀 <b>TOP PUMP — CÓ THỂ TĂNG MẠNH (1D)</b>")
    lines.append("═══════════════════════════\n")

    pump_rank_styles = [
        ("🟢🥇", "TOP 1 PUMP — ƯU TIÊN MẠNH"),
        ("🟡🥈", "TOP 2 PUMP — THEO DÕI"),
    ]

    if pump_results:
        for i, r in enumerate(pump_results[:2]):
            badge, rank_name = pump_rank_styles[i] if i < len(pump_rank_styles) else ("⭐", "WATCHLIST")
            if r.market_mode == "SQUEEZE":
                mode_icon = "🔴"
            elif r.market_mode == "TREND":
                mode_icon = "🟢"
            elif r.market_mode == "HYBRID":
                mode_icon = "🟣"
            else:
                mode_icon = "⚡"

            symbol   = html.escape(r.display_symbol)   # "BTC" không có sàn
            signal   = html.escape(str(r.signal_type))
            engine_info = ""
            if r.market_mode in ("SQUEEZE", "HYBRID"):
                engine_info = f" | 🔴SQ:{r.squeeze_engine_score:.1f}"
            elif r.market_mode == "TREND":
                engine_info = f" | 🟢TR:{r.trend_score:.1f}"

            def pct(t, e):
                if e <= 0: return ""
                return f"{(t-e)/e*100:+.2f}%"

            lines.append(
                f"{badge} <b>{rank_name}</b>\n"
                f"<b>{symbol}</b> — <b>{r.total_score:.1f}đ</b>{engine_info}\n"
                f"{mode_icon} <b>{signal}</b>\n"
                f"💰 Giá: <b>{r.price_current:.6g}</b> | +{r.price_chg:.2f}%"
            )
            if r.entry > 0 and r.tp1 > 0:
                lines.append(
                    f"📍 Entry: <b>{r.entry:.6g}</b> | SL: <b>{r.sl:.6g}</b>\n"
                    f"🎯 TP1: <b>{r.tp1:.6g}</b> ({pct(r.tp1, r.entry)})\n"
                    f"🎯 TP2: <b>{r.tp2:.6g}</b> ({pct(r.tp2, r.entry)})\n"
                    f"🎯 TP3: <b>{r.tp3:.6g}</b> ({pct(r.tp3, r.entry)})"
                )
            lines.append("")
    else:
        lines.append("<i>Không tìm thấy coin pump đủ điều kiện.</i>\n")

    # ── DUMP SECTION ──────────────────────────────────────────────
    lines.append("═══════════════════════════")
    lines.append("📉 <b>TOP DUMP — CÓ THỂ GIẢM MẠNH (1D)</b>")
    lines.append("═══════════════════════════\n")

    dump_rank_styles = [
        ("🔴🥇", "TOP 1 DUMP — CẨN THẬN CAO"),
        ("🟠🥈", "TOP 2 DUMP — THEO DÕI"),
    ]

    if dump_results:
        for i, r in enumerate(dump_results[:2]):
            badge, rank_name = dump_rank_styles[i] if i < len(dump_rank_styles) else ("⭐", "WATCHLIST")
            symbol   = html.escape(r.display_symbol)
            signal   = html.escape(str(r.signal_type))

            def pct_d(t, e):
                if e <= 0: return ""
                return f"{(t-e)/e*100:+.2f}%"

            lines.append(
                f"{badge} <b>{rank_name}</b>\n"
                f"<b>{symbol}</b> — <b>{r.total_score:.1f}đ</b>\n"
                f"📉 <b>{signal}</b>\n"
                f"💰 Giá: <b>{r.price_current:.6g}</b> | {r.price_chg:.2f}%"
            )
            if r.entry > 0 and r.tp1 > 0:
                lines.append(
                    f"📍 Entry: <b>{r.entry:.6g}</b> | SL: <b>{r.sl:.6g}</b>\n"
                    f"🎯 TP1: <b>{r.tp1:.6g}</b> ({pct_d(r.tp1, r.entry)})\n"
                    f"🎯 TP2: <b>{r.tp2:.6g}</b> ({pct_d(r.tp2, r.entry)})\n"
                    f"🎯 TP3: <b>{r.tp3:.6g}</b> ({pct_d(r.tp3, r.entry)})"
                )
            lines.append("")
    else:
        lines.append("<i>Không tìm thấy coin dump đủ điều kiện.</i>\n")

    lines.append("⚠️ <i>Không phải lời khuyên đầu tư. Luôn đặt SL.</i>")
    return "\n".join(lines)


def save_results(pump_results: list[ScoreResult], dump_results: list[ScoreResult],
                 rev_results: list[ScoreResult]) -> str:
    import os
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    filename = f"results/scan_multi_{timestamp}.json"
    os.makedirs("results", exist_ok=True)

    def to_dict(r: ScoreResult, direction: str) -> dict:
        return {
            "direction": direction,
            "timeframe": r.timeframe,
            "exchange": r.exchange,
            "symbol": r.symbol,
            "score": r.total_score,
            "signal_type": r.signal_type,
            "reversal_type": r.reversal_type,
            "vol_ratio": r.vol_ratio,
            "oi_chg_pct": r.oi_chg_pct,
            "fr": r.fr,
            "lsr": r.lsr,
            "liq_ratio": r.liq_ratio,
            "price_chg": r.price_chg,
            "h1_chg": r.h1_chg,
            "price_current": r.price_current,
            "day_low": r.day_low,
            "entry": r.entry,
            "sl": r.sl,
            "tp1": r.tp1,
            "tp2": r.tp2,
            "tp3": r.tp3,
            "rr_tp1": r.rr_tp1,
            "rr_tp2": r.rr_tp2,
            "market_mode": r.market_mode,
            "trend_score": r.trend_score,
            "squeeze_engine_score": r.squeeze_engine_score,
            "details": r.details,
            "scores": {
                "momentum": r.score_momentum,
                "cvb": r.score_cvb,
                "oi_div": r.score_oi_div,
                "fr": r.score_fr,
                "lsr": r.score_lsr,
                "liq": r.score_liq,
                "squeeze": r.score_squeeze,
            }
        }

    data = (
        [to_dict(r, "PUMP") for r in pump_results] +
        [to_dict(r, "DUMP") for r in dump_results] +
        [to_dict(r, r.reversal_type or "REVERSAL") for r in rev_results]
    )

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    log.info(f"💾 Kết quả lưu: {filename}")
    return filename


# ══════════════════════════════════════════════════════════════
# MAIN JOB
# ══════════════════════════════════════════════════════════════

def run_reversal_scan() -> list[ScoreResult]:
    """
    Chỉ scan Reversal (1H) — dùng cho job 30 phút.
    4 sàn chạy SONG SONG, dedup, trả về top 1 mỗi loại.
    """
    all_rev: list[ScoreResult] = []
    scan_start = time.time()

    def _scan_exchange_reversal(exchange: str) -> list[ScoreResult]:
        symbols = get_all_symbols(exchange)
        if not symbols:
            return []
        workers = (MAX_WORKERS_BINANCE if exchange == "Binance"
                   else MAX_WORKERS_BINGX if exchange == "BingX"
                   else MAX_WORKERS_KUCOIN if exchange == "KuCoin"
                   else MAX_WORKERS_BYBIT)
        log.info(f"🔄 Reversal scan {exchange}: {len(symbols)} symbols...")
        results = []
        completed = 0
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {executor.submit(_reversal_only, exchange, s): s for s in symbols}
            for future in as_completed(future_map):
                completed += 1
                try:
                    rev = future.result()
                    if rev:
                        results.append(rev)
                except Exception as e:
                    log.debug(f"  ❌ {exchange} {future_map[future]}: {e}")
                if completed % LOG_EVERY_N == 0 or completed == len(symbols):
                    log.info(f"  [{exchange}] {completed}/{len(symbols)} | rev {len(results)}")
        log.info(f"✅ {exchange} reversal xong: {len(results)} signals | {time.time()-scan_start:.0f}s")
        return results

    # 4 sàn song song
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_EXCHANGES) as ex_pool:
        futures = {ex_pool.submit(_scan_exchange_reversal, ex): ex for ex in SCAN_EXCHANGES}
        for future in as_completed(futures):
            try:
                all_rev.extend(future.result())
            except Exception as e:
                log.error(f"❌ Reversal scan error {futures[future]}: {e}")

    log.info(f"⏱️ Reversal parallel scan xong: {time.time()-scan_start:.1f}s | {len(all_rev)} total")

    # Dedup — KuCoin USDTM → strip M trước khi so sánh
    seen: dict[str, ScoreResult] = {}
    for r in sorted(all_rev, key=lambda x: x.total_score, reverse=True):
        base = r.symbol.upper()
        if base.endswith("USDTM"):
            base = base[:-1]
        if base not in seen:
            seen[base] = r
    unique = list(seen.values())

    result: list[ScoreResult] = []
    for rtype in ("PUMP_REVERSAL", "DUMP_REVERSAL", "H1_BREAKOUT_LONG", "H1_BREAKOUT_SHORT"):
        filtered = [r for r in unique if r.reversal_type == rtype]
        if filtered:
            result.append(max(filtered, key=lambda x: x.total_score))
    return result


def _reversal_only(exchange: str, symbol: str) -> Optional[ScoreResult]:
    """Helper: fetch coin data và chạy score_reversal + score_h1_breakout."""
    coin = fetch_coin_data(exchange, symbol)
    if coin is None:
        return None
    rev1 = score_reversal(coin)
    rev2 = score_h1_breakout(coin)
    if rev1 and rev2:
        return rev1 if rev1.total_score >= rev2.total_score else rev2
    return rev1 or rev2


def format_reversal_alert(rev_results: list[ScoreResult]) -> str:
    """Alert gọn cho scan 30 phút — Reversal signals với M30 xác nhận + TP/SL."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"🔄 <b>REVERSAL UPDATE — {now}</b>",
        f"📊 {' | '.join(SCAN_EXCHANGES)} — 1H + M30\n",
    ]

    for r in rev_results:
        symbol   = html.escape(r.display_symbol)
        signal   = html.escape(str(r.signal_type))

        if r.reversal_type == "PUMP_REVERSAL":
            rev_icon  = "🔴🔄"
            rev_label = "PUMP → SHORT"
            chg_line  = f"1D: +{r.price_chg:.2f}% | 1H: {r.h1_chg:.2f}%"
        elif r.reversal_type == "DUMP_REVERSAL":
            rev_icon  = "🟢🔄"
            rev_label = "DUMP → LONG"
            chg_line  = f"1D: {r.price_chg:.2f}% | 1H: +{r.h1_chg:.2f}%"
        elif r.reversal_type == "H1_BREAKOUT_LONG":
            rev_icon  = "🟢⚡"
            rev_label = "H1 BREAKOUT — LONG"
            chg_line  = f"1H: +{r.h1_chg:.2f}% | 1D: {r.price_chg:+.2f}%"
        else:  # H1_BREAKOUT_SHORT
            rev_icon  = "🔴⚡"
            rev_label = "H1 BREAKOUT — SHORT"
            chg_line  = f"1H: {r.h1_chg:.2f}% | 1D: {r.price_chg:+.2f}%"

        # M30 status tag
        if r.m30_confirmed:
            m30_tag = f"✅ M30: {r.m30_chg:+.2f}% (xác nhận)"
        elif r.m30_chg != 0:
            m30_tag = f"⚠️ M30: {r.m30_chg:+.2f}% (chưa xác nhận)"
        else:
            m30_tag = "M30: N/A"

        # H1 time remaining tag
        if r.h1_minutes_left > 0:
            h1_tag = f"⏱️ H1 còn ~{r.h1_minutes_left}phút"
        else:
            h1_tag = "⏱️ H1 vừa đóng"

        def pct(target: float, entry: float) -> str:
            if entry <= 0: return ""
            return f"{(target - entry) / entry * 100:+.2f}%"

        lines.append(f"{rev_icon} <b>{rev_label} — {symbol}</b> — <b>{r.total_score:.1f}đ</b>")
        lines.append(f"⚡ <b>{signal}</b>")
        lines.append(f"💰 {chg_line}")
        lines.append(f"{m30_tag} | {h1_tag} | FR: {r.fr:.4f}%")

        if r.entry > 0 and r.tp1 > 0:
            lines.append(
                f"📍 Entry: <b>{r.entry:.6g}</b> | SL: <b>{r.sl:.6g}</b>\n"
                f"🎯 TP1: <b>{r.tp1:.6g}</b> ({pct(r.tp1, r.entry)}) R:R {r.rr_tp1:.1f}\n"
                f"🎯 TP2: <b>{r.tp2:.6g}</b> ({pct(r.tp2, r.entry)}) R:R {r.rr_tp2:.1f}\n"
                f"🎯 TP3: <b>{r.tp3:.6g}</b> ({pct(r.tp3, r.entry)})"
            )
        lines.append("")

    lines.append("⚠️ <i>Không phải lời khuyên đầu tư. Luôn đặt SL.</i>")
    return "\n".join(lines)


def job_reversal_only():
    """Job chạy lúc xx:32 UTC — chỉ scan và gửi Reversal nếu có signal."""
    try:
        rev_results = run_reversal_scan()

        if not rev_results:
            log.info("🔄 Reversal scan xong — không có signal mới.")
            return  # Không gửi Telegram nếu không có gì

        log.info(f"🔄 Reversal scan xong — {len(rev_results)} signal(s):")
        for r in rev_results:
            log.info(f"  {r.reversal_type}: {r.display_symbol} {r.total_score:.1f}đ — {r.signal_type}")
            log.info(f"  1D: {r.price_chg:+.2f}% | 1H: {r.h1_chg:+.2f}%")

        msg = format_reversal_alert(rev_results)
        if send_telegram(msg):
            log.info("✅ Reversal alert đã gửi!")
        else:
            log.error("❌ Gửi Telegram thất bại!")

    except Exception as e:
        log.error(f"job_reversal_only error: {e}", exc_info=True)


def _score_candle(o: float, h: float, l: float, c: float,
                  vol: float, vol_ma: float, label: str) -> tuple[float, str]:
    """
    Score 1 nến đơn theo chiều PUMP.
    Trả về (score, direction_icon).
    """
    if o <= 0 or vol_ma <= 0:
        return 0.0, "❓"
    chg = (c - o) / o * 100
    vr  = vol / vol_ma if vol_ma > 0 else 0
    score = 0.0
    if chg >= 20 and vr >= 2:   score = 3.0
    elif chg >= 12 and vr >= 1.5: score = 2.5
    elif chg >= 20:               score = 2.0
    elif chg >= 12:               score = 1.5
    elif chg >= 5:                score = 1.0
    elif chg >= 2:                score = 0.5
    elif chg >= -2:               score = 0.0   # sideway
    elif chg >= -5:               score = -0.5
    else:                         score = -1.0

    if chg >= 2:    icon = "🟢"
    elif chg >= -2: icon = "🟡"
    else:           icon = "🔴"
    return score, icon


@dataclass
class MTFResult:
    """Kết quả scan đa khung D + H12 + H6."""
    symbol:   str
    exchange: str
    # Scores từng khung
    score_d:   float = 0
    score_h12: float = 0
    score_h6:  float = 0
    # Icons từng khung
    icon_d:   str = "❓"
    icon_h12: str = "❓"
    icon_h6:  str = "❓"
    # Thay đổi % từng khung
    chg_d:   float = 0
    chg_h12: float = 0
    chg_h6:  float = 0
    # Vol ratio
    vr_d:  float = 0
    vr_h6: float = 0
    # Data cho TP/SL
    d_open:  float = 0
    d_high:  float = 0
    d_low:   float = 0
    d_close: float = 0
    # TP/SL
    entry: float = 0
    sl:    float = 0
    tp1:   float = 0
    tp2:   float = 0
    tp3:   float = 0
    # Tổng
    mtf_score:   float = 0
    green_count: int   = 0   # Số khung xanh / 3
    signal_type: str   = ""
    direction:   str   = "PUMP"

    @property
    def display_symbol(self) -> str:
        s = self.symbol
        if s.endswith("USDTM"): return s[:-5]
        if s.endswith("USDT"):  return s[:-4]
        return s


def score_coin_mtf(exchange: str, symbol: str) -> Optional[MTFResult]:
    """
    Quét đa khung D + H12 + H6 cho 1 coin.
    Trọng số: D=50%, H12=20%, H6=30%
    Chỉ dùng cho daily scan 00:02 UTC.
    """
    candles_d = get_ohlcv(exchange, symbol, limit=25)
    if not candles_d or len(candles_d) < 3:
        return None

    latest_d = candles_d[-1]
    d_o = float(latest_d.get("o", 0)); d_h = float(latest_d.get("h", 0))
    d_l = float(latest_d.get("l", 0)); d_c = float(latest_d.get("c", 0))
    d_v = float(latest_d.get("v", 0))
    if d_o <= 0 or d_c <= 0:
        return None

    prev_vols = [float(c.get("v", 0)) for c in candles_d[-11:-1]]
    vol_ma_d  = sum(prev_vols) / len(prev_vols) if prev_vols else 0
    if vol_ma_d <= 0:
        return None

    d_chg = (d_c - d_o) / d_o * 100
    d_vr  = d_v / vol_ma_d

    if abs(d_chg) < 5.0 and d_vr < 1.5:
        return None

    result = MTFResult(symbol=symbol, exchange=exchange)
    result.d_open  = d_o; result.d_high  = d_h
    result.d_low   = d_l; result.d_close = d_c
    result.chg_d   = round(d_chg, 2)
    result.vr_d    = round(d_vr, 2)
    result.direction = "PUMP" if d_chg >= 0 else "DUMP"

    score_d, icon_d = _score_candle(d_o, d_h, d_l, d_c, d_v, vol_ma_d, "D")
    result.score_d = score_d; result.icon_d = icon_d

    # ── H12 ──────────────────────────────────────────────────────
    candles_h12 = get_ohlcv_h12(exchange, symbol, limit=6)
    if candles_h12 and len(candles_h12) >= 3:
        h12 = candles_h12[-1]
        h12_vols = [float(c.get("v", 0)) for c in candles_h12[-4:-1]]
        h12_ma   = sum(h12_vols) / len(h12_vols) if h12_vols else 0
        s, ic = _score_candle(float(h12.get("o",0)), float(h12.get("h",0)),
                               float(h12.get("l",0)), float(h12.get("c",0)),
                               float(h12.get("v",0)), h12_ma, "H12")
        result.score_h12 = s; result.icon_h12 = ic
        o12 = float(h12.get("o", 1))
        result.chg_h12 = round((float(h12.get("c",o12)) - o12) / o12 * 100, 2) if o12 > 0 else 0

    # ── H6 ───────────────────────────────────────────────────────
    candles_h6 = get_ohlcv_h6(exchange, symbol, limit=8)
    if candles_h6 and len(candles_h6) >= 3:
        h6 = candles_h6[-1]
        h6_vols = [float(c.get("v", 0)) for c in candles_h6[-5:-1]]
        h6_ma   = sum(h6_vols) / len(h6_vols) if h6_vols else 0
        s, ic = _score_candle(float(h6.get("o",0)), float(h6.get("h",0)),
                               float(h6.get("l",0)), float(h6.get("c",0)),
                               float(h6.get("v",0)), h6_ma, "H6")
        result.score_h6 = s; result.icon_h6 = ic
        result.vr_h6    = round(float(h6.get("v",0)) / h6_ma, 2) if h6_ma > 0 else 0
        o6 = float(h6.get("o", 1))
        result.chg_h6   = round((float(h6.get("c",o6)) - o6) / o6 * 100, 2) if o6 > 0 else 0

    # ── MTF score: D=50%, H12=20%, H6=30% ────────────────────────
    result.mtf_score = round(
        result.score_d   * 0.50 +
        result.score_h12 * 0.20 +
        result.score_h6  * 0.30,
        2
    )

    # Đếm số khung cùng chiều (3 khung)
    if result.direction == "PUMP":
        result.green_count = sum(1 for ic in [result.icon_d, result.icon_h12, result.icon_h6] if ic == "🟢")
    else:
        result.green_count = sum(1 for ic in [result.icon_d, result.icon_h12, result.icon_h6] if ic == "🔴")

    # Cần ít nhất 2/3 khung cùng chiều
    if result.green_count < 2:
        return None

    # ── TP / SL từ range nến D ────────────────────────────────────
    d_range = d_h - d_l
    if d_range > 0:
        result.entry = round(d_c, 8)
        if result.direction == "PUMP":
            result.sl  = round(d_l - d_range * 0.1, 8)
            result.tp1 = round(d_c + d_range * 0.5,   8)
            result.tp2 = round(d_c + d_range * 1.0,   8)
            result.tp3 = round(d_c + d_range * 1.618, 8)
        else:
            result.sl  = round(d_h + d_range * 0.1, 8)
            result.tp1 = round(d_c - d_range * 0.5,   8)
            result.tp2 = round(d_c - d_range * 1.0,   8)
            result.tp3 = round(d_c - d_range * 1.618, 8)

    # Signal label
    gc = result.green_count
    if gc == 3:
        result.signal_type = "🔥 3/3 KHUNG — MUA MẠNH NHẤT" if result.direction == "PUMP" else "🔥 3/3 KHUNG — SHORT MẠNH NHẤT"
    else:
        result.signal_type = "✅ 2/3 KHUNG — MUA TỐT" if result.direction == "PUMP" else "✅ 2/3 KHUNG — SHORT TỐT"

    return result


def run_mtf_scan() -> tuple[list[MTFResult], list[MTFResult]]:
    """
    Quét đa khung 4 sàn song song.
    Trả về (pump_top2, dump_top2).
    """
    all_pump: list[MTFResult] = []
    all_dump:  list[MTFResult] = []
    scan_start = time.time()

    def _scan_exchange_mtf(exchange: str) -> list[MTFResult]:
        symbols = get_all_symbols(exchange)
        if not symbols:
            return []
        workers = (MAX_WORKERS_BINANCE if exchange == "Binance"
                   else MAX_WORKERS_BINGX if exchange == "BingX"
                   else MAX_WORKERS_KUCOIN if exchange == "KuCoin"
                   else MAX_WORKERS_BYBIT)
        log.info(f"📅 MTF scan {exchange}: {len(symbols)} symbols...")
        results = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            fmap = {executor.submit(score_coin_mtf, exchange, s): s for s in symbols}
            for future in as_completed(fmap):
                try:
                    r = future.result()
                    if r:
                        results.append(r)
                except Exception as e:
                    log.debug(f"MTF {exchange} {fmap[future]}: {e}")
        log.info(f"✅ MTF {exchange}: {len(results)} signals | {time.time()-scan_start:.0f}s")
        return results

    # 4 sàn song song
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_EXCHANGES) as ex_pool:
        futures = {ex_pool.submit(_scan_exchange_mtf, ex): ex for ex in SCAN_EXCHANGES}
        for future in as_completed(futures):
            try:
                for r in future.result():
                    if r.direction == "PUMP":
                        all_pump.append(r)
                    else:
                        all_dump.append(r)
            except Exception as e:
                log.error(f"MTF scan error: {e}")

    # Dedup
    def dedup_mtf(lst: list[MTFResult]) -> list[MTFResult]:
        seen: dict[str, MTFResult] = {}
        # Sort: green_count cao trước, rồi abs(mtf_score) cao trước
        for r in sorted(lst, key=lambda x: (x.green_count, abs(x.mtf_score)), reverse=True):
            base = r.symbol.upper()
            if base.endswith("USDTM"): base = base[:-1]
            if base not in seen:
                seen[base] = r
        return list(seen.values())

    unique_pump = dedup_mtf(all_pump)
    unique_dump = dedup_mtf(all_dump)

    # Sort pump: green_count cao → mtf_score cao
    unique_pump.sort(key=lambda x: (x.green_count, x.mtf_score), reverse=True)
    # Sort dump: green_count cao → mtf_score âm nhất (abs cao nhất) = dump mạnh nhất
    unique_dump.sort(key=lambda x: (x.green_count, abs(x.mtf_score)), reverse=True)

    log.info(f"📅 MTF scan xong: {time.time()-scan_start:.1f}s | pump {len(unique_pump)} | dump {len(unique_dump)}")
    return unique_pump[:1], unique_dump[:1]


def format_mtf_alert(pump_list: list[MTFResult], dump_list: list[MTFResult]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"📅 <b>DAILY MTF SCAN — {now}</b>\n"]

    def fmt_coin(r: MTFResult, section_icon: str, section_label: str) -> None:
        sym = html.escape(r.display_symbol)
        def pct(t, e):
            if e <= 0: return ""
            return f"{(t-e)/e*100:+.2f}%"
        lines.append(f"{'═'*27}")
        lines.append(f"{section_icon} <b>{section_label}</b>")
        lines.append(f"{'═'*27}\n")
        lines.append(
            f"<b>{sym}</b> — <b>{r.mtf_score:.2f}đ</b> | {r.green_count}/3 khung\n"
            f"⚡ <b>{html.escape(r.signal_type)}</b>\n"
            f"D: {r.icon_d}{r.chg_d:+.1f}% | H12: {r.icon_h12}{r.chg_h12:+.1f}% | H6: {r.icon_h6}{r.chg_h6:+.1f}%\n"
            f"Vol D: {r.vr_d:.1f}x | Vol H6: {r.vr_h6:.1f}x"
        )
        if r.entry > 0 and r.tp1 > 0:
            lines.append(
                f"📍 Entry: <b>{r.entry:.6g}</b> | SL: <b>{r.sl:.6g}</b>\n"
                f"🎯 TP1: <b>{r.tp1:.6g}</b> ({pct(r.tp1, r.entry)})\n"
                f"🎯 TP2: <b>{r.tp2:.6g}</b> ({pct(r.tp2, r.entry)})\n"
                f"🎯 TP3: <b>{r.tp3:.6g}</b> ({pct(r.tp3, r.entry)})"
            )
        lines.append("")

    if pump_list:
        fmt_coin(pump_list[0], "🚀", "TOP MUA NGÀY HÔM NAY")
    else:
        lines.append(f"{'═'*27}\n🚀 <b>TOP MUA NGÀY HÔM NAY</b>\n{'═'*27}\n")
        lines.append("<i>Không có signal pump đủ 2/3 khung.</i>\n")

    if dump_list:
        fmt_coin(dump_list[0], "📉", "TOP SHORT/DUMP NGÀY HÔM NAY")
    else:
        lines.append(f"{'═'*27}\n📉 <b>TOP SHORT/DUMP NGÀY HÔM NAY</b>\n{'═'*27}\n")
        lines.append("<i>Không có signal dump đủ 2/3 khung.</i>\n")

    lines.append("⚠️ <i>Không phải lời khuyên đầu tư. Luôn đặt SL.</i>")
    return "\n".join(lines)


# ── daily_watch.json — track coin để HOLD/OUT check ──────────

DAILY_WATCH_FILE = "daily_watch.json"

def save_daily_watch(pump: list[MTFResult], dump: list[MTFResult]) -> None:
    import os, json as _json
    os.makedirs("results", exist_ok=True)
    data = {
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "pump": [{"symbol": r.symbol, "exchange": r.exchange,
                  "entry": r.entry, "sl": r.sl,
                  "tp1": r.tp1, "tp2": r.tp2, "tp3": r.tp3,
                  "d_high": r.d_high, "d_low": r.d_low,
                  "mtf_score": r.mtf_score, "green_count": r.green_count} for r in pump],
        "dump": [{"symbol": r.symbol, "exchange": r.exchange,
                  "entry": r.entry, "sl": r.sl,
                  "tp1": r.tp1, "tp2": r.tp2, "tp3": r.tp3,
                  "d_high": r.d_high, "d_low": r.d_low,
                  "mtf_score": r.mtf_score, "green_count": r.green_count} for r in dump],
    }
    with open(f"results/{DAILY_WATCH_FILE}", "w") as f:
        _json.dump(data, f, ensure_ascii=False, indent=2)
    log.info(f"💾 Daily watch saved: pump {len(pump)} | dump {len(dump)}")


def load_daily_watch() -> dict:
    import json as _json
    try:
        with open(f"results/{DAILY_WATCH_FILE}") as f:
            return _json.load(f)
    except Exception:
        return {}


def job_daily_mtf():
    """00:02 UTC — Quét MTF, alert top 2 pump + dump, lưu watch list."""
    try:
        log.info("📅 Daily MTF scan bắt đầu...")
        pump, dump = run_mtf_scan()

        if not pump and not dump:
            log.info("📅 Không có MTF signal đủ điều kiện.")
            send_telegram("📅 Daily MTF scan xong — không có signal đủ 2/4 khung.")
            return

        log.info(f"📅 MTF results: pump {len(pump)} | dump {len(dump)}")
        for r in pump:
            log.info(f"  🟢 {r.display_symbol} {r.green_count}/3 {r.mtf_score:.2f}đ D:{r.chg_d:+.1f}% H6:{r.chg_h6:+.1f}%")
        for r in dump:
            log.info(f"  🔴 {r.display_symbol} {r.green_count}/3 {r.mtf_score:.2f}đ D:{r.chg_d:+.1f}% H6:{r.chg_h6:+.1f}%")

        save_daily_watch(pump, dump)
        msg = format_mtf_alert(pump, dump)
        if send_telegram(msg):
            log.info("✅ Daily MTF alert đã gửi!")
        else:
            log.error("❌ Gửi Daily MTF alert thất bại!")

    except Exception as e:
        log.error(f"job_daily_mtf error: {e}", exc_info=True)
        send_telegram(f"❌ Daily MTF error: {html.escape(str(e))}")


def job_hold_check():
    """
    04:02, 08:02, 12:02, 16:02, 20:02 UTC — Check HOLD/OUT cho coin đã alert.
    Dựa trên nến H4 mới nhất.
    """
    watch = load_daily_watch()
    if not watch or (not watch.get("pump") and not watch.get("dump")):
        log.info("🔍 Hold check: không có coin trong watch list.")
        return

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if watch.get("date") != today:
        log.info(f"🔍 Hold check: watch list ngày {watch.get('date')} ≠ hôm nay {today} — bỏ qua.")
        return

    now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")
    lines   = [f"🔍 <b>HOLD/OUT CHECK — {now_str}</b>\n"]
    has_signal = False

    def check_coin(item: dict, direction: str) -> Optional[str]:
        symbol   = item["symbol"]
        exchange = item["exchange"]
        entry    = item["entry"]
        sl       = item["sl"]
        tp1      = item["tp1"]
        tp2      = item["tp2"]
        tp3      = item["tp3"]

        # Lấy nến H6 mới nhất (check mỗi 6H nên dùng H6)
        h6_candles = get_ohlcv_h6(exchange, symbol, limit=6)
        if not h6_candles or len(h6_candles) < 3:
            return None

        h6 = h6_candles[-1]
        h6_o = float(h6.get("o", 0)); h6_c = float(h6.get("c", 0))
        h6_h = float(h6.get("h", 0)); h6_l = float(h6.get("l", 0))
        h6_v = float(h6.get("v", 0))
        h6_vols = [float(c.get("v", 0)) for c in h6_candles[-4:-1]]
        h6_ma   = sum(h6_vols) / len(h6_vols) if h6_vols else 1
        h6_chg  = (h6_c - h6_o) / h6_o * 100 if h6_o > 0 else 0
        h6_vr   = h6_v / h6_ma if h6_ma > 0 else 0

        cur_price = h6_c
        pnl = (cur_price - entry) / entry * 100 if entry > 0 else 0
        if direction == "DUMP": pnl = -pnl

        # TP/SL check — dùng H6 high/low để bắt TP chính xác hơn
        check_price_high = h6_h if direction == "PUMP" else h6_l
        check_price_low  = h6_l if direction == "PUMP" else h6_h

        def pct(t, e): return f"{(t-e)/e*100:+.2f}%" if e > 0 else ""

        # Xác định verdict
        if direction == "PUMP":
            if check_price_high >= tp3 and tp3 > 0:
                verdict = f"🎯🎯🎯 TP3 HIT! ({pct(tp3, entry)})"
                urgent  = True
            elif check_price_high >= tp2 and tp2 > 0:
                verdict = f"🎯🎯 TP2 HIT! ({pct(tp2, entry)}) — Cân nhắc chốt"
                urgent  = True
            elif check_price_high >= tp1 and tp1 > 0:
                verdict = f"🎯 TP1 HIT ({pct(tp1, entry)}) — Di SL lên entry"
                urgent  = True
            elif check_price_low <= sl:
                verdict = f"🛑 SL HIT ({pct(sl, entry)})"
                urgent  = True
            elif h6_chg <= -3 and h6_vr >= 1.5:
                verdict = f"⚠️ OUT — H6 đỏ {h6_chg:.1f}% vol {h6_vr:.1f}x"
                urgent  = True
            elif h6_chg >= 2 and h6_vr >= 1.2:
                verdict = f"✅ HOLD — H6 {h6_chg:+.1f}% vol {h6_vr:.1f}x"
                urgent  = False
            else:
                verdict = f"✅ HOLD — Chờ (H6 {h6_chg:+.1f}%)"
                urgent  = False
        else:  # DUMP
            if check_price_low <= tp3 and tp3 > 0:
                verdict = f"🎯🎯🎯 TP3 HIT! ({pct(tp3, entry)})"
                urgent  = True
            elif check_price_low <= tp2 and tp2 > 0:
                verdict = f"🎯🎯 TP2 HIT! ({pct(tp2, entry)})"
                urgent  = True
            elif check_price_low <= tp1 and tp1 > 0:
                verdict = f"🎯 TP1 HIT ({pct(tp1, entry)})"
                urgent  = True
            elif check_price_high >= sl:
                verdict = f"🛑 SL HIT ({pct(sl, entry)})"
                urgent  = True
            elif h6_chg >= 3 and h6_vr >= 1.5:
                verdict = f"⚠️ OUT — H6 xanh {h6_chg:+.1f}% vol {h6_vr:.1f}x"
                urgent  = True
            else:
                verdict = f"✅ HOLD — H6 {h6_chg:+.1f}%"
                urgent  = False

        sym = symbol[:-5] if symbol.endswith("USDTM") else symbol[:-4] if symbol.endswith("USDT") else symbol
        icon = "🟢" if direction == "PUMP" else "🔴"
        return (
            f"{icon} <b>{sym}</b> {direction} | PnL: <b>{pnl:+.2f}%</b>\n"
            f"{verdict}\n"
            f"Giá: {cur_price:.6g} | TP1:{tp1:.6g} TP2:{tp2:.6g} TP3:{tp3:.6g}"
        ), urgent

    urgent_lines = []
    for item in watch.get("pump", []):
        try:
            result = check_coin(item, "PUMP")
            if result:
                msg_text, urgent = result
                lines.append(msg_text); lines.append("")
                if urgent: urgent_lines.append(msg_text)
                has_signal = True
        except Exception as e:
            log.debug(f"Hold check pump {item.get('symbol')}: {e}")

    for item in watch.get("dump", []):
        try:
            result = check_coin(item, "DUMP")
            if result:
                msg_text, urgent = result
                lines.append(msg_text); lines.append("")
                if urgent: urgent_lines.append(msg_text)
                has_signal = True
        except Exception as e:
            log.debug(f"Hold check dump {item.get('symbol')}: {e}")

    if not has_signal:
        log.info("🔍 Hold check: không lấy được H6 data.")
        return

    msg = "\n".join(lines)
    if send_telegram(msg):
        log.info("✅ Hold check alert đã gửi!")
    else:
        log.error("❌ Hold check gửi thất bại!")

    # Gửi thêm alert riêng nếu có TP hit
    if urgent_lines:
        urgent_msg = "🚨 <b>URGENT — TP/SL HIT!</b>\n\n" + "\n\n".join(urgent_lines)
        send_telegram(urgent_msg)
        log.info(f"🚨 Urgent alert gửi: {len(urgent_lines)} signal(s)")


def run_h2_scan() -> tuple[list[ScoreResult], list[ScoreResult]]:
    """Quét H2 song song 4 sàn. Trả về (pump_top1, dump_top1)."""
    all_pump: list[ScoreResult] = []
    all_dump:  list[ScoreResult] = []
    scan_start = time.time()

    def _scan_exchange_h2(exchange: str) -> list[ScoreResult]:
        symbols = get_all_symbols(exchange)
        if not symbols: return []
        workers = (MAX_WORKERS_BINANCE if exchange == "Binance"
                   else MAX_WORKERS_BINGX if exchange == "BingX"
                   else MAX_WORKERS_KUCOIN if exchange == "KuCoin"
                   else MAX_WORKERS_BYBIT)
        log.info(f"⚡ H2 scan {exchange}: {len(symbols)} symbols...")
        results = []
        with ThreadPoolExecutor(max_workers=workers) as executor:
            fmap = {executor.submit(score_coin_h2, exchange, s): s for s in symbols}
            for future in as_completed(fmap):
                try:
                    r = future.result()
                    if r: results.append(r)
                except Exception as e:
                    log.debug(f"H2 {exchange} {fmap[future]}: {e}")
        log.info(f"✅ H2 {exchange}: {len(results)} signals | {time.time()-scan_start:.0f}s")
        return results

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_EXCHANGES) as ex_pool:
        futures = {ex_pool.submit(_scan_exchange_h2, ex): ex for ex in SCAN_EXCHANGES}
        for future in as_completed(futures):
            try:
                for r in future.result():
                    if "PUMP" in r.market_mode:
                        all_pump.append(r)
                    else:
                        all_dump.append(r)
            except Exception as e:
                log.error(f"H2 scan error: {e}")

    # Dedup
    def dedup_h2(lst: list[ScoreResult]) -> list[ScoreResult]:
        seen: dict[str, ScoreResult] = {}
        for r in sorted(lst, key=lambda x: x.total_score, reverse=True):
            base = r.symbol.upper()
            if base.endswith("USDTM"): base = base[:-1]
            if base not in seen: seen[base] = r
        return list(seen.values())

    pumps = dedup_h2(all_pump)
    dumps = dedup_h2(all_dump)
    pumps.sort(key=lambda x: x.total_score, reverse=True)
    dumps.sort(key=lambda x: x.total_score, reverse=True)

    log.info(f"⚡ H2 scan xong: {time.time()-scan_start:.1f}s | pump {len(pumps)} | dump {len(dumps)}")
    return pumps[:1], dumps[:1]


def format_h2_alert(pump: list[ScoreResult], dump: list[ScoreResult]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"⚡ <b>H2 SCAN — {now}</b>\n"]

    def fmt_r(r: ScoreResult, section: str) -> None:
        sym    = html.escape(r.display_symbol)
        signal = html.escape(r.signal_type)
        icon   = "🟢" if "PUMP" in r.market_mode else "🔴"

        def pct(t, e):
            if e <= 0: return ""
            return f"{(t-e)/e*100:+.2f}%"

        lines.append(f"{'═'*27}")
        lines.append(f"{icon} <b>{section}</b>")
        lines.append(f"{'═'*27}\n")
        lines.append(
            f"<b>{sym}</b> — <b>{r.total_score:.1f}đ</b>\n"
            f"⚡ <b>{signal}</b>\n"
            f"H2: {r.price_chg:+.2f}% | Vol: {r.vol_ratio:.1f}x | "
            f"OI: {r.oi_chg_pct:+.1f}% | FR: {r.fr:.4f}%"
        )
        if r.entry > 0 and r.tp1 > 0:
            lines.append(
                f"📍 Entry: <b>{r.entry:.6g}</b> | SL: <b>{r.sl:.6g}</b>\n"
                f"🎯 TP1: <b>{r.tp1:.6g}</b> ({pct(r.tp1, r.entry)}) R:R {r.rr_tp1:.1f}\n"
                f"🎯 TP2: <b>{r.tp2:.6g}</b> ({pct(r.tp2, r.entry)}) R:R {r.rr_tp2:.1f}\n"
                f"🎯 TP3: <b>{r.tp3:.6g}</b> ({pct(r.tp3, r.entry)})"
            )
        lines.append("")

    if pump:
        fmt_r(pump[0], "H2 PUMP — MUA NGẮN HẠN")
    if dump:
        fmt_r(dump[0], "H2 DUMP — SHORT NGẮN HẠN")
    if not pump and not dump:
        lines.append("<i>Không có H2 signal đủ điều kiện.</i>")

    lines.append("⚠️ <i>Target 15-30%. Luôn đặt SL chặt.</i>")
    return "\n".join(lines)


def job_h2_scan():
    """Chạy mỗi 2H — scan H2 pump/dump, alert nếu có signal."""
    try:
        pump, dump = run_h2_scan()
        if not pump and not dump:
            log.info("⚡ H2 scan xong — không có signal.")
            return
        msg = format_h2_alert(pump, dump)
        if send_telegram(msg):
            log.info(f"✅ H2 alert gửi: pump {len(pump)} dump {len(dump)}")
        else:
            log.error("❌ H2 alert gửi thất bại!")
    except Exception as e:
        log.error(f"job_h2_scan error: {e}", exc_info=True)


def job():
    try:
        pump_results, dump_results, rev_results = run_scan()

        if not pump_results and not dump_results and not rev_results:
            log.warning("Không có coin nào đủ điều kiện!")
            send_telegram("⚠️ Scan xong nhưng không tìm thấy coin nào đủ điều kiện.")
            return

        log.info("\n" + "=" * 60)
        log.info("🏆 KẾT QUẢ — PUMP")
        log.info("=" * 60)
        for i, r in enumerate(pump_results, 1):
            log.info(f"{i}. {r.display_symbol}: {r.total_score:.1f}đ — {r.market_mode} — {r.signal_type}")
            for d in r.details:
                log.info(f"   {d}")

        log.info("\n" + "=" * 60)
        log.info("💀 KẾT QUẢ — DUMP")
        log.info("=" * 60)
        for i, r in enumerate(dump_results, 1):
            log.info(f"{i}. {r.display_symbol}: {r.total_score:.1f}đ — {r.market_mode} — {r.signal_type}")
            for d in r.details:
                log.info(f"   {d}")

        log.info("\n" + "=" * 60)
        log.info("🔄 KẾT QUẢ — REVERSAL")
        log.info("=" * 60)
        for i, r in enumerate(rev_results, 1):
            log.info(f"{i}. {r.display_symbol}: {r.total_score:.1f}đ — {r.reversal_type} — {r.signal_type}")
            log.info(f"   1D: {r.price_chg:+.2f}% | 1H: {r.h1_chg:+.2f}%")
            for d in r.details:
                log.info(f"   {d}")

        save_results(pump_results, dump_results, rev_results)
        msg = format_alert(pump_results, dump_results, rev_results)
        if send_telegram(msg):
            log.info("✅ Đã gửi Telegram alert!")
        else:
            log.error("❌ Gửi Telegram thất bại!")

    except Exception as e:
        log.error(f"Job error: {e}", exc_info=True)
        send_telegram(f"❌ Scanner error: {e}")


# ══════════════════════════════════════════════════════════════
# ENTRY POINT
# ══════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import sys

    if len(sys.argv) > 1 and sys.argv[1] == "--now":
        log.info("🧪 Chạy test ngay...")
        job()

    elif len(sys.argv) > 1 and sys.argv[1] == "--test-one":
        if len(sys.argv) < 3:
            print("Cách dùng: python bot.py --test-one SYMBOL [Binance|Bybit]")
            raise SystemExit(1)

        symbol = sys.argv[2].upper()
        exchange = sys.argv[3] if len(sys.argv) > 3 else "Binance"
        if exchange.lower() == "bybit":
            exchange = "Bybit"
        else:
            exchange = "Binance"

        log.info(f"🧪 Test coin: {symbol} · {exchange}")
        coin = fetch_coin_data(exchange, symbol)
        if coin:
            result = score_coin(coin)
            if result:
                print(f"\n✅ {result.display_symbol}: {result.total_score:.1f}đ")
                print(f"Mode: {result.market_mode} | Trend: {result.trend_score} | Squeeze: {result.squeeze_engine_score}")
                print(f"Signal: {result.signal_type}")
                for d in result.details:
                    print(f"  {d}")
            else:
                print(f"⚠️ {symbol} · {exchange}: Điểm thấp hơn ngưỡng {MIN_SCORE}")
        else:
            print(f"❌ Không lấy được data cho {symbol} · {exchange}")

    else:
        # ── SCHEDULER: 2 loại scan mỗi 30 phút ──────────────────
        #
        #   xx:02 UTC → Full scan (PUMP + DUMP + REVERSAL) → gửi đầy đủ
        #   xx:32 UTC → Reversal-only scan → gửi nếu có signal, im lặng nếu không
        #
        #   Nếu ENABLE_30MIN_SCAN = False → chỉ chạy xx:02 như cũ
        #
        #   Anti-overlap: nếu scan kéo dài qua mốc kế tiếp → bỏ mốc đó,
        #   chờ mốc tiếp theo để tránh chạy chồng.

        from datetime import timedelta

        FULL_SCAN_MINUTE = 2
        REV_SCAN_MINUTE  = 32
        CHECK_SLEEP      = 30
        HOLD_CHECK_HOURS = {6, 12, 18}      # Hold/Out check mỗi 6H
        H2_SCAN_HOURS_SET = set(range(2, 24, 2))  # 02,04,06...22 UTC — H2 scan mỗi 2H

        def next_slot_utc(now=None):
            now = now or datetime.now(timezone.utc)
            base = now.replace(second=0, microsecond=0)
            candidates = []

            # daily 00:02 UTC
            s = base.replace(hour=0, minute=FULL_SCAN_MINUTE)
            if s <= now: s += timedelta(days=1)
            candidates.append((s, "daily"))

            # hold check 06:02, 12:02, 18:02 UTC
            for h in HOLD_CHECK_HOURS:
                s = base.replace(hour=h, minute=FULL_SCAN_MINUTE)
                if s <= now: s += timedelta(days=1)
                candidates.append((s, "hold_check"))

            # H2 scan 02:02, 04:02, 06:02... 22:02 UTC
            for h in H2_SCAN_HOURS_SET:
                s = base.replace(hour=h, minute=FULL_SCAN_MINUTE)
                if s <= now: s += timedelta(days=1)
                candidates.append((s, "h2_scan"))

            # full scan: xx:02 các giờ lẻ không trùng daily/hold/h2
            occupied = {0} | HOLD_CHECK_HOURS | H2_SCAN_HOURS_SET
            s = base.replace(minute=FULL_SCAN_MINUTE)
            if s <= now: s += timedelta(hours=1)
            while s.hour in occupied:
                s += timedelta(hours=1)
            candidates.append((s, "full"))

            candidates.sort(key=lambda x: x[0])
            return candidates[0]

        def slot_id(dt, kind):
            return f"{dt.strftime('%Y%m%d%H%M')}_{kind}"

        log.info("⏰ SCHEDULER V5 khởi động")
        log.info("   00:02 UTC           → DAILY MTF scan (D+H12+H6 — 1 lần/ngày)")
        log.info("   02/04/06...22:02    → H2 scan (pump/dump H2 — mỗi 2H)")
        log.info("   06/12/18:02 UTC     → HOLD/OUT check (H6 — mỗi 6H)")
        log.info("   xx:02 giờ lẻ        → Full scan 1D (mỗi giờ còn lại)")

        last_executed_slot = ""

        while True:
            target, slot_kind = next_slot_utc()

            while True:
                now = datetime.now(timezone.utc)
                wait = (target - now).total_seconds()
                if wait <= 0: break
                labels = {"daily":"Daily MTF","hold_check":"Hold/Out","full":"Full scan","reversal":"Reversal"}
                log.info(f"⏳ Đợi {wait/60:.1f}p đến {target.strftime('%H:%M UTC')} ({labels.get(slot_kind,slot_kind)})...")
                time.sleep(min(wait, CHECK_SLEEP))

            current_slot = slot_id(target, slot_kind)
            if current_slot == last_executed_slot:
                log.warning(f"⚠️ Slot {current_slot} đã chạy — bỏ qua")
                time.sleep(60); continue

            scan_start_dt = datetime.now(timezone.utc)
            scan_start    = time.time()
            last_executed_slot = current_slot

            try:
                if slot_kind == "daily":
                    log.info(f"📅 [{scan_start_dt.strftime('%H:%M UTC')}] Daily MTF scan...")
                    job_daily_mtf()
                elif slot_kind == "hold_check":
                    log.info(f"🔍 [{scan_start_dt.strftime('%H:%M UTC')}] Hold/Out check...")
                    job_hold_check()
                elif slot_kind == "h2_scan":
                    log.info(f"⚡ [{scan_start_dt.strftime('%H:%M UTC')}] H2 scan...")
                    job_h2_scan()
                elif slot_kind == "full":
                    log.info(f"🚀 [{scan_start_dt.strftime('%H:%M UTC')}] Full scan 1D...")
                    job()
                else:
                    pass  # Reversal scan đã tắt
            except Exception as e:
                log.error(f"Scheduler [{slot_kind}]: {e}", exc_info=True)
                try: send_telegram(f"❌ [{slot_kind}] error: {html.escape(str(e))}")
                except Exception: pass

            elapsed     = time.time() - scan_start
            finished_dt = datetime.now(timezone.utc)
            next_target, next_kind = next_slot_utc(finished_dt)
            labels = {"daily":"Daily MTF","hold_check":"Hold/Out",
                      "h2_scan":"H2 Scan","full":"Full 1D"}
            log.info(f"✅ Xong {elapsed:.0f}s — tiếp theo: {next_target.strftime('%H:%M UTC')} ({labels.get(next_kind,next_kind)})")
