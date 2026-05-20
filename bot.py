#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
╔==============================================================╗
|  CRYPTO PUMP & DUMP SCANNER BOT V5                          |
|  Quét USDT Perp: Binance, Bybit                             |
|  1D Trend/Squeeze + 1H Reversal Engine -> Telegram           |
╚==============================================================╝
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
SCAN_EXCHANGES = ["Binance", "Bybit"]  # Chỉ quét Binance + Bybit, bỏ BingX/KuCoin để tránh lệch giá và signal nhiễu
PER_EXCHANGE_TOP_N = False             # False = gộp cả 3 sàn rồi xếp điểm cao xuống thấp
TOP_N_FINAL = 3                         # Chỉ gửi 3 coin tiềm năng nhất
AUTO_SCAN_INTERVAL_SECONDS = 3600       # Scan tự động mỗi 1 giờ
MIN_VOL_RATIO_FILTER = 2.0              # Tăng 1.2->2.0: loại noise MOG/1INCH vol thấp (PUMP)
MIN_PRICE_CHANGE_FILTER = 5.0           # Loại coin tăng quá yếu nếu volume không đủ (PUMP)
MAX_LSR_HEALTHY = 2.30                  # L/S quá cao = crowded long, giảm điểm

# Ngưỡng riêng cho DUMP -- thấp hơn pump vì dump không cần vol spike mạnh
MIN_DUMP_VOL_RATIO = 0.8               # Vol tối thiểu để xét dump (0.8 = không cần spike)
MIN_DUMP_PRICE_DROP = 3.0              # Drop tối thiểu 3% để lọt vào dump scan
MIN_DUMP_SCORE = 3.0                   # Ngưỡng điểm tối thiểu để lọt top dump

# -- Institutional Distribution / Post-Squeeze SHORT Engine -----
ENABLE_DISTRIBUTION_ENGINE = True
MIN_DISTRIBUTION_SCORE = 5.0
H6_BREAKDOWN_MIN_DROP = 8.0          # H6 giảm >= 8% sau blowoff = cảnh báo short
H12_BREAKDOWN_MIN_DROP = 10.0        # H12 giảm >= 10% = cảnh báo short
DAILY_BLOWOFF_UPPER_WICK_RATIO = 0.45 # râu trên / range ngày >= 45%
OI_ROLLOVER_MIN_PCT = -3.0           # OI giảm >= 3% sau spike = rollover
DEADCAT_RETRACE_MIN = 0.382          # Entry zone short: hồi 38.2% nhịp dump
DEADCAT_RETRACE_MAX = 0.618          # Entry zone short: hồi 61.8% nhịp dump

# -- 1H Reversal Engine ----------------------------------------
ENABLE_1H_REVERSAL = False             # TẮT reversal scan -- chỉ dùng PUMP + DUMP + H4 MTF
ENABLE_30MIN_SCAN = False              # TẮT scan reversal mỗi 30 phút
DAILY_SCAN_HOUR   = 0                  # Giờ UTC chạy full scan 1D (0 = 00:02 UTC)
# Pump Reversal: coin pump mạnh 1D nhưng 1H đang đảo chiều xuống
PUMP_REV_1D_MIN_PUMP = 10.0           # 1D tăng tối thiểu 10% trước đó
PUMP_REV_1H_DROP = 3.0                # 1H hiện tại giảm >= 3%
PUMP_REV_1H_VOL_MULT = 1.5            # Vol 1H >= 1.5x MA10_1H
# Dump Reversal: coin dump mạnh 1D nhưng 1H đang bật ngược lên
DUMP_REV_1D_MIN_DUMP = 8.0            # 1D giảm tối thiểu 8% trước đó
DUMP_REV_1H_PUMP = 3.0                # 1H hiện tại tăng >= 3%
DUMP_REV_1H_VOL_MULT = 1.5            # Vol 1H >= 1.5x MA10_1H
INTRADAY_DUMP_MIN = 15.0              # Intraday dump (open->low) >= 15% trong nến ngày hiện tại
MIN_REVERSAL_SCORE = 3.0              # Điểm tối thiểu để lọt reversal list

# REVERSAL output rule: lấy top 2 LONG + top 2 SHORT điểm cao nhất.
# Ưu tiên Binance/Bybit khi điểm gần nhau để tránh lệch giá/spread ở sàn nhỏ.
REVERSAL_TOP_PER_SIDE = 2
REVERSAL_PRIORITY_EXCHANGES = {"Binance": 2, "Bybit": 2}
REVERSAL_PRIORITY_SCORE_BONUS = 0.25

# -- H4 Multi-Timeframe Pump Scanner --------------------------
# Bắt pump sớm trong ngày: scan H4 mỗi 4 giờ, tích điểm từ H4+H12+1H
# Ví dụ: EDEN ngày 17/5 -- H12 đầu +11.82% vol 4.6x -> đủ trigger
ENABLE_H4_MTF_SCAN    = True
H4_MTF_MIN_CHG        = 8.0    # H4 tăng tối thiểu 8%
H4_MTF_MIN_VOL        = 3.0    # vol_ratio H4 tối thiểu 3x MA10
H4_MTF_MIN_SCORE      = 5.0    # Điểm tối thiểu để alert
H4_MTF_SCAN_INTERVAL  = 4      # Chạy mỗi 4 giờ: 00:05, 04:05, 08:05, 12:05, 16:05, 20:05 UTC
# Bonus điểm khi có xác nhận đa khung
H4_MTF_H12_CONFIRM_BONUS  = 1.5  # H12 cùng màu xanh + vol tăng -> +1.5đ
H4_MTF_H1_CONFIRM_BONUS   = 1.0  # H1 tăng > 5% xác nhận momentum -> +1.0đ
H4_MTF_FR_SQUEEZE_BONUS   = 1.5  # FR âm (short squeeze) -> +1.5đ
H4_MTF_OI_EXPAND_BONUS    = 1.0  # OI tăng > 15% xác nhận có tiền thật vào -> +1.0đ

# -- H4 Watch List -- tier thấp hơn, bắt nến bật đáy kiểu PROMPT --
# PROMPT 19/5: H4 +6.12% vol 2.85x -> không lọt H4 MTF nhưng đáng theo dõi
# Tiêu chí: % thấp hơn + vol thấp hơn + OI tăng + FR neutral
ENABLE_H4_WATCHLIST       = True
H4_WATCH_MIN_CHG          = 4.0   # H4 tăng tối thiểu 4% (thấp hơn MTF)
H4_WATCH_MAX_CHG          = 9.0   # Không vượt ngưỡng MTF (tránh trùng)
H4_WATCH_MIN_VOL          = 1.8   # vol_ratio tối thiểu 1.8x (thấp hơn MTF)
H4_WATCH_MAX_VOL          = 3.5   # Không vượt ngưỡng MTF
H4_WATCH_MIN_SCORE        = 3.0   # Ngưỡng điểm thấp -- Watch List nên inclusive
H4_WATCH_OI_MIN           = 3.0   # OI tăng tối thiểu 3% để xác nhận có tiền vào
H4_WATCH_FR_MAX           = 0.05  # FR không được quá dương (không crowded long)
H4_WATCH_MAX_COINS        = 3     # Chỉ hiện top 3 coin Watch List


H2_MIN_VOL      = 1.3    # vol_ratio H2 tối thiểu (thấp hơn D vì H2 vol hay thấp)
H2_MIN_SCORE    = 4.0    # ngưỡng điểm (thấp hơn D=5.0)
H2_SCAN_HOURS   = 2      # quét mỗi 2H

# -- 1H Momentum Breakout --------------------------------------
# Signal độc lập với 1D -- bắt nến 1H pump/dump mạnh có vol spike
# Ví dụ: MLNUSDT 07:00 UTC 14/5 -- +10.37% vol 9.3x FR âm
H1_BREAKOUT_MIN_CHG     = 8.0    # 1H thay đổi tối thiểu (pump hoặc dump)
H1_BREAKOUT_MIN_VOL     = 5.0    # vol_ratio 1H tối thiểu (x MA10)
H1_BREAKOUT_MIN_SCORE   = 4.0    # Điểm tối thiểu để alert
H1_BREAKOUT_FR_BONUS    = -0.05  # FR âm <= ngưỡng này -> bonus squeeze

# Engine mode
TREND_MIN_SCORE = 5.0                   # Ngưỡng nhận diện TREND coin kiểu IRYS
SQUEEZE_MIN_SCORE = 5.0                 # Ngưỡng nhận diện SQUEEZE coin kiểu COS
HYBRID_MIN_SCORE = 5.0                  # Cả trend + squeeze đều mạnh

# Tăng tốc scan
FAST_SCAN = True
MAX_WORKERS_BINANCE = 12   # Giữ -- Binance weight-based, 12 là sweet spot
MAX_WORKERS_BYBIT  = 15   # Bybit limit 120 req/s, còn dư nhiều
MAX_WORKERS_BINGX  = 6    # BingX limit 10 req/s thực tế
MAX_WORKERS_KUCOIN = 5    # 5 workers + delay 80ms -> ~10-12 req/s, safe với 30 req/min thực tế
KUCOIN_REQUEST_DELAY = 0.08  # 80ms delay giữa các request -> ~12 req/s max

# Số workers tối đa cho parallel exchange scan (3 sàn chạy đồng thời)
MAX_WORKERS_EXCHANGES = 2  # Chạy Binance + Bybit song song
LOG_EVERY_N = 25           # Log tiến độ mỗi N coin thay vì in từng coin

# -- Logging --------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("scanner.log", encoding="utf-8")
    ]
)
log = logging.getLogger(__name__)

# -- API Base -------------------------------------------------
COINGLASS_BASE = "https://open-api-v3.coinglass.com/api"
BINANCE_BASE = "https://fapi.binance.com"
BYBIT_BASE  = "https://api.bybit.com"
BINGX_BASE  = "https://open-api.bingx.com"
KUCOIN_BASE = "https://api-futures.kucoin.com"   # KuCoin Futures public API

# Rate limiter cho KuCoin -- semaphore + delay để tránh 429
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


# ==============================================================
# DATA CLASSES
# ==============================================================

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
    # Intraday dump: (open - low) / open -- bắt case dump sâu trong ngày rồi bật lại
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
    # M30 data -- xác nhận momentum cho reversal scan 30 phút
    m30_open: float = 0
    m30_close: float = 0
    m30_high: float = 0
    m30_low: float = 0
    m30_volume: float = 0
    m30_vol_ma10: float = 0
    m30_price_change_pct: float = 0
    m30_prev_change_pct: float = 0     # nến M30 trước đó (để xem trend M30)
    m30_available: bool = False
    # H4 data -- Multi-Timeframe Pump Scanner
    h4_open: float = 0
    h4_close: float = 0
    h4_high: float = 0
    h4_low: float = 0
    h4_volume: float = 0
    h4_vol_ma10: float = 0
    h4_price_change_pct: float = 0
    h4_available: bool = False
    # H12 data -- xác nhận momentum nền cho H4 MTF scan
    h12_open: float = 0
    h12_close: float = 0
    h12_high: float = 0
    h12_low: float = 0
    h12_volume: float = 0
    h12_vol_ma5: float = 0
    h12_price_change_pct: float = 0
    h12_available: bool = False
    # H2 data -- MTF scanner nhanh hơn H4, bắt pump 2 giờ sớm hơn (kiểu HIGH 18/4)
    h2_open: float = 0
    h2_close: float = 0
    h2_high: float = 0
    h2_low: float = 0
    h2_volume: float = 0
    h2_vol_ma10: float = 0
    h2_price_change_pct: float = 0
    h2_available: bool = False

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
    # Pullback detection fields
    has_pullback: bool = False       # True nếu phát hiện có cú hồi đang diễn ra
    pullback_pct: float = 0          # % hồi so với range (0-100%)
    pullback_type: str = ""          # "RETRACING" | "CONTINUING" | "UNKNOWN"
    limit_entry_fib: float = 0       # Entry limit tính theo Fibonacci retracement
    details: list = field(default_factory=list)

    @property
    def display_symbol(self) -> str:
        s = self.symbol
        # KuCoin format: BTCUSDTM -> BTC
        if s.endswith("USDTM"):
            base = s[:-5]
        elif s.endswith("USDT"):
            base = s[:-4]
        else:
            base = s
        return f"{base} . {self.exchange}"


# ==============================================================
# GENERIC HTTP
# ==============================================================

def http_get(url: str, params: dict | None = None, headers: dict | None = None, timeout: int = 15) -> Optional[Any]:
    for attempt in range(3):
        try:
            r = get_session().get(url, params=params or {}, headers=headers or {}, timeout=timeout)
            r.raise_for_status()
            return r.json()
        except requests.RequestException as e:
            log.warning(f"GET failed ({attempt+1}/3): {url} -- {e}")
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


# ==============================================================
# SYMBOLS
# ==============================================================

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
        symbol = item.get("symbol", "").replace("-", "")  # BTC-USDT -> BTCUSDT
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


# ==============================================================
# KUCOIN API
# ==============================================================

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
                    log.debug(f"KuCoin 429 -> đợi {wait:.0f}s rồi retry ({attempt+1}/3)...")
                    time.sleep(wait)
                    continue
                r.raise_for_status()
                time.sleep(KUCOIN_REQUEST_DELAY)  # throttle sau mỗi success
                return r.json()
            except requests.RequestException as e:
                if attempt < 2:
                    time.sleep(1.5 * (attempt + 1))
                else:
                    log.debug(f"KuCoin GET failed (3/3): {url} -- {e}")
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
    """KuCoin Futures public API helper -- có throttle."""
    data = _kucoin_http(f"{KUCOIN_BASE}{endpoint}", params=params or {}, timeout=15)
    return _kucoin_parse_response(data)


def kucoin_get_quick(endpoint: str, params: dict | None = None) -> Optional[Any]:
    """KuCoin quick (timeout ngắn hơn) -- có throttle."""
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
    """KuCoin kline -> chuẩn hóa dict. Newest-first -> cần reverse sau."""
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
    """OI snapshot từ contract info -- KuCoin không có daily hist public."""
    data = kucoin_get_quick(f"/api/v1/contracts/{symbol}")
    if data and isinstance(data, dict):
        oi = data.get("openInterest") or data.get("openInterestValue") or 0
        return float(oi)
    return None
# OHLCV PUBLIC API
# ==============================================================

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
    """BingX OHLCV -- symbol format: BTCUSDT -> BTC-USDT cho API."""
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


# -- 1H OHLCV -------------------------------------------------

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


# -- M30 OHLCV ------------------------------------------------

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


# -- H6 / H12 OHLCV (cho MTF Daily scan) ---------------------

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


def get_ohlcv_h4(exchange: str, symbol: str, limit: int = 12) -> Optional[list]:
    return _get_ohlcv_interval(exchange, symbol, "4h", "240", "4h", 240, limit)





# ==============================================================
# FUNDING / OI / LSR / LIQUIDATION
# Ưu tiên Binance/Bybit public API để tránh Coinglass bị 500.
# Coinglass chỉ dùng phụ cho Liquidation nếu bật USE_COINGLASS_LIQ.
# ==============================================================

USE_COINGLASS_LIQ = True   # Bật để lấy liquidation -- quan trọng cho scoring


def http_get_quick(url: str, params: dict | None = None, headers: dict | None = None, timeout: int = 8) -> Optional[Any]:
    """GET nhanh, chỉ thử 1 lần. Dùng cho endpoint phụ để bot không bị kẹt."""
    try:
        r = get_session().get(url, params=params or {}, headers=headers or {}, timeout=timeout)
        r.raise_for_status()
        return r.json()
    except requests.RequestException as e:
        log.debug(f"Quick GET failed: {url} -- {e}")
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
        # KuCoin không có daily OI hist public -> dùng snapshot
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

    # BingX: không có LSR public endpoint -> trả None
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


# ==============================================================
# DATA FETCHER
# ==============================================================

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

    # Lookback 2 nến ngày trước -- cho Reversal Engine
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

    # Intraday dump depth: (open - low) / open -- nến ngày hiện tại
    # Case MLNUSDT: open=3.157, low=2.073 -> dump 34.3% trong ngày dù close chưa phản ánh hết
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

    # -- 1H data cho Reversal Engine ------------------------------
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

    # -- M30 data -- xác nhận momentum cho reversal scan -----------
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

    # -- H4 + H12 data cho MTF Pump Scanner ----------------------
    if ENABLE_H4_MTF_SCAN:
        # H4 candles
        h4_candles = get_ohlcv_h4(exchange, symbol, limit=14)
        if h4_candles and len(h4_candles) >= 12:
            h4_latest = h4_candles[-1]
            coin.h4_open   = float(h4_latest.get("o", 0))
            coin.h4_close  = float(h4_latest.get("c", 0))
            coin.h4_high   = float(h4_latest.get("h", 0))
            coin.h4_low    = float(h4_latest.get("l", 0))
            coin.h4_volume = float(h4_latest.get("v", 0))
            if coin.h4_open > 0:
                coin.h4_price_change_pct = (coin.h4_close - coin.h4_open) / coin.h4_open * 100
            prev_h4_vols = [float(c.get("v", 0)) for c in h4_candles[-11:-1]]
            coin.h4_vol_ma10 = sum(prev_h4_vols) / len(prev_h4_vols) if prev_h4_vols else 0
            coin.h4_available = True

        # H12 candles
        h12_candles = get_ohlcv_h12(exchange, symbol, limit=8)
        if h12_candles and len(h12_candles) >= 4:
            h12_latest = h12_candles[-1]
            coin.h12_open   = float(h12_latest.get("o", 0))
            coin.h12_close  = float(h12_latest.get("c", 0))
            coin.h12_high   = float(h12_latest.get("h", 0))
            coin.h12_low    = float(h12_latest.get("l", 0))
            coin.h12_volume = float(h12_latest.get("v", 0))
            if coin.h12_open > 0:
                coin.h12_price_change_pct = (coin.h12_close - coin.h12_open) / coin.h12_open * 100
            prev_h12_vols = [float(c.get("v", 0)) for c in h12_candles[-6:-1]]
            coin.h12_vol_ma5 = sum(prev_h12_vols) / len(prev_h12_vols) if prev_h12_vols else 0
            coin.h12_available = True

        # H2 candles -- bắt pump sớm hơn H4 2 giờ (kiểu HIGH 18/4: H2 close 0.2112 vs H4 close 0.2445)
        h2_candles = get_ohlcv_h2(exchange, symbol, limit=14)
        if h2_candles and len(h2_candles) >= 12:
            h2_latest = h2_candles[-1]
            coin.h2_open   = float(h2_latest.get("o", 0))
            coin.h2_close  = float(h2_latest.get("c", 0))
            coin.h2_high   = float(h2_latest.get("h", 0))
            coin.h2_low    = float(h2_latest.get("l", 0))
            coin.h2_volume = float(h2_latest.get("v", 0))
            if coin.h2_open > 0:
                coin.h2_price_change_pct = (coin.h2_close - coin.h2_open) / coin.h2_open * 100
            prev_h2_vols = [float(c.get("v", 0)) for c in h2_candles[-11:-1]]
            coin.h2_vol_ma10 = sum(prev_h2_vols) / len(prev_h2_vols) if prev_h2_vols else 0
            coin.h2_available = True

    return coin


# ==============================================================
# SCORING ENGINE
# ==============================================================


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

def detect_pullback(result: ScoreResult, coin: CoinData, side: str) -> None:
    """
    Phát hiện lực hồi trên khung M30/1H để quyết định Entry Now hay Entry Limit.

    Logic:
    ---------------------------------------------------------------------
    SHORT signal (DUMP / PUMP_REVERSAL):
      * Nếu M30 hoặc 1H đang xanh (giá hồi lên từ đáy) -> có lực hồi
        -> has_pullback = True, dùng Entry Limit (chờ hồi xong mới short)
        -> limit_entry_fib = close + (high - close) * 0.382 (hồi 38.2%)
      * Nếu M30 tiếp tục đỏ, momentum chưa hồi -> Entry Now luôn

    LONG signal (REVERSAL LONG):
      * Nếu M30 đang đỏ sau khi bật lên (pullback từ đỉnh 1H) -> có lực hồi nhỏ
        -> has_pullback = True, Entry Limit thấp hơn (chờ hồi xuống vào)
        -> limit_entry_fib = close - (close - low) * 0.382
      * Nếu M30 xanh liên tiếp -> Entry Now

    Kết quả ghi vào: result.has_pullback, result.pullback_pct,
                     result.pullback_type, result.limit_entry_fib
    ---------------------------------------------------------------------
    """
    import math

    def _sig_digits(v: float) -> float:
        if v <= 0:
            return 0.0
        digits = max(2, -int(math.floor(math.log10(abs(v)))) + 3)
        return round(v, digits)

    # -- Lấy dữ liệu M30 / 1H --------------------------------------------
    m30_chg      = coin.m30_price_change_pct if coin.m30_available else 0.0
    m30_prev_chg = coin.m30_prev_change_pct  if coin.m30_available else 0.0
    h1_chg       = coin.h1_price_change_pct  if coin.h1_available  else 0.0

    # Tỷ lệ hồi tương đối so với range nến ngày
    d_range  = coin.high - coin.low   if coin.high > coin.low  else 0
    h1_range = coin.h1_high - coin.h1_low if coin.h1_available and coin.h1_high > coin.h1_low else 0

    if side == "SHORT":
        # -- SHORT: phát hiện hồi lên ------------------------------------
        # Điều kiện có lực hồi: M30 đang xanh, HOẶC 1H đang xanh (giá bật từ đáy)
        m30_retracing = coin.m30_available and m30_chg > 0.5   # M30 đang xanh > 0.5%
        h1_retracing  = coin.h1_available  and h1_chg  > 1.0   # 1H đang xanh > 1%

        if m30_retracing or h1_retracing:
            result.has_pullback   = True
            result.pullback_type  = "RETRACING"

            # Tính % hồi so với range ngày
            rebound_chg = max(m30_chg if m30_retracing else 0, h1_chg if h1_retracing else 0)
            result.pullback_pct = round(rebound_chg, 2)

            # Entry Limit = giá hiện tại + hồi thêm theo Fibonacci 38.2% range ngày
            # Ý nghĩa: chờ giá hồi lên vùng 38.2% rồi short, không entry now
            if d_range > 0 and coin.close > 0:
                fib_382 = coin.close + d_range * 0.382
                fib_500 = coin.close + d_range * 0.500
                # Chọn vùng giữa: hồi 38.2%-50% là vùng short an toàn
                result.limit_entry_fib = _sig_digits(fib_382)

        else:
            result.has_pullback  = False
            result.pullback_type = "CONTINUING"  # Dump đang tiếp tục, entry now

    else:  # LONG
        # -- LONG: phát hiện hồi xuống (sau khi bật) ---------------------
        # Nếu M30 đang đỏ sau khi 1H bật lên = giá đang hồi nhỏ -> có thể chờ vào rẻ hơn
        m30_pulling_back = coin.m30_available and m30_chg < -0.5  # M30 đỏ > 0.5%
        m30_two_red      = coin.m30_available and m30_chg < 0 and m30_prev_chg < 0

        if m30_pulling_back and coin.h1_available and h1_chg > 2.0:
            # 1H xanh mạnh nhưng M30 đang kéo lại = pullback nhỏ
            result.has_pullback   = True
            result.pullback_type  = "RETRACING"
            result.pullback_pct   = round(abs(m30_chg), 2)

            # Entry Limit = giá hiện tại - hồi xuống theo Fibonacci 23.6%-38.2% range 1H
            if h1_range > 0 and coin.h1_close > 0:
                fib_236 = coin.h1_close - h1_range * 0.236
                result.limit_entry_fib = _sig_digits(max(fib_236, coin.h1_close * 0.95))

        else:
            result.has_pullback  = False
            result.pullback_type = "CONTINUING"  # Đang bật mạnh, entry now


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


def calc_dynamic_tp(result: ScoreResult, coin: CoinData,
                     swing_low: float = 0, swing_high: float = 0,
                     timeframe: str = "1D") -> None:
    """
    Tính TP động dựa trên momentum score + Fibonacci extension.

    Nguyên lý:
    -----------------------------------------------------------------
    Thay vì TP = entry + N*range (cố định), dùng Fibonacci extension
    từ swing_low -> entry -> projected_high:

      Fib 1.618 = entry + (entry - swing_low) * 1.618   <- TP1 conservative
      Fib 2.618 = entry + (entry - swing_low) * 2.618   <- TP2 standard
      Fib 4.236 = entry + (entry - swing_low) * 4.236   <- TP3 aggressive

    Momentum multiplier (từ score + vol + FR):
      * score >= 8 + vol >= 5x + FR âm sâu -> multiplier = 1.5 -> TP xa hơn
      * score 6-8 + vol 3-5x              -> multiplier = 1.2
      * mặc định                           -> multiplier = 1.0

    Ví dụ EDEN H12 (entry=0.0431, swing_low=0.0379, vol=4.6x, FR=-0.0478%, score~8đ):
      swing_move = 0.0431 - 0.0379 = 0.0052
      TP1 = 0.0431 + 0.0052 * 1.618 * 1.2 = 0.0431 + 0.0101 ≈ 0.0532 (+23%)
      TP2 = 0.0431 + 0.0052 * 2.618 * 1.2 = 0.0431 + 0.0163 ≈ 0.0594 (+38%)
      TP3 = 0.0431 + 0.0052 * 4.236 * 1.2 = 0.0431 + 0.0264 ≈ 0.0695 (+61%)
      (Thực tế đỉnh 0.0929 = +115%, TP3 vẫn conservative nhưng tốt hơn range cũ)
    -----------------------------------------------------------------
    """
    import math

    def _fmt(v: float) -> float:
        if v <= 0: return 0.0
        digits = max(2, -int(math.floor(math.log10(abs(v)))) + 3)
        return round(v, digits)

    entry = result.entry
    if entry <= 0:
        return

    is_long = (result.price_chg >= 0)

    # -- Xác định swing_low / swing_high --------------------------
    if is_long:
        base = swing_low if swing_low > 0 else (coin.h4_low if coin.h4_available and coin.h4_low > 0 else coin.low)
        swing_move = entry - base
    else:
        base = swing_high if swing_high > 0 else (coin.h4_high if coin.h4_available and coin.h4_high > 0 else coin.high)
        swing_move = base - entry

    if swing_move <= 0:
        return   # Fallback về TP cũ nếu không có swing

    # -- Momentum multiplier --------------------------------------
    score     = result.total_score
    vol_ratio = result.vol_ratio
    fr_pct    = result.fr   # đã là %

    if score >= 8 and vol_ratio >= 4 and fr_pct <= -0.03:
        mult = 1.5    # Squeeze mạnh kiểu EDEN -- TP rất xa
        tp_label = "🔥 Momentum cực mạnh"
    elif score >= 7 and vol_ratio >= 3:
        mult = 1.3    # Momentum tốt
        tp_label = "💪 Momentum mạnh"
    elif score >= 6:
        mult = 1.1
        tp_label = "📈 Momentum trung bình"
    else:
        mult = 1.0
        tp_label = ""

    # -- Fibonacci extension TP -----------------------------------
    if is_long:
        result.tp1 = _fmt(entry + swing_move * 1.618  * mult)
        result.tp2 = _fmt(entry + swing_move * 2.618  * mult)
        result.tp3 = _fmt(entry + swing_move * 4.236  * mult)
    else:
        result.tp1 = _fmt(entry - swing_move * 1.618  * mult)
        result.tp2 = _fmt(entry - swing_move * 2.618  * mult)
        result.tp3 = _fmt(entry - swing_move * 4.236  * mult)

    # Ghi chú vào details
    if tp_label and hasattr(result, 'details') and result.details is not None:
        tp1_pct = abs(result.tp1 - entry) / entry * 100
        tp3_pct = abs(result.tp3 - entry) / entry * 100
        result.details.append(
            f"{tp_label} (x{mult}) -- TP1 +{tp1_pct:.0f}% / TP3 +{tp3_pct:.0f}% (Fib ext)"
        )


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

    # 0. Momentum -- 2 tier:
    #
    #   Tier 1 (vol xác nhận): cả price lẫn vol đều mạnh -> pump bền, điểm cao nhất
    #   Tier 2 (thin air):     pump mạnh dù vol thấp hơn MA -> vẫn alert, điểm thấp hơn 0.5đ
    #   Ví dụ: VELVET +19.8% vol 0.2x, AIN +34.7% vol 0.29x, QUSDT +34% vol 0.29x
    #          PIEVERSEUSDT +26.4% vol 1.58x -> tất cả đáng alert

    if chg >= 20 and vol_ratio >= 2:
        result.score_momentum = 3.0
        details.append(f"🚀 Momentum mạnh (+{chg:.1f}% vol {vol_ratio:.1f}x)")
    elif chg >= 12 and vol_ratio >= 1.5:
        result.score_momentum = 2.0
        details.append(f"🚀 Momentum (+{chg:.1f}% vol {vol_ratio:.1f}x)")
    elif chg >= 30:                          # thin air pump cực mạnh (>=30%)
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
        # Bù bằng OI tăng -> xác nhận có lực mua thật
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

    # 5. Liquidation direction (pump: shorts bị liq nhiều hơn -> tốt)
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

    # 7. Vol-confirmed bonus -- bù khi OI thấp/không có data
    #    Vol >= 2x + pump >= 12% mà không có OI signal = lực mua thật từ spot/market
    #    Ví dụ: UBUSDT +15.38% vol 2.1x OI +8.5% -> vừa miss, cần bonus này
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
    # Dynamic TP: Fibonacci extension từ swing_low 20D -> entry
    swing_low = coin.low_20d if coin.low_20d > 0 else coin.low
    calc_dynamic_tp(result, coin, swing_low=swing_low, timeframe="1D")
    detect_pullback(result, coin, "LONG")
    return result


def _score_mtf_pump_candle(
    coin: CoinData,
    tf_label: str,
    tf_open: float, tf_close: float, tf_high: float, tf_low: float,
    tf_volume: float, tf_vol_ma10: float,
    confirm_green: bool, confirm_vol_r: float,
) -> Optional[ScoreResult]:
    """Generic MTF pump scorer -- dùng chung cho H2, H4, H6..."""
    if tf_vol_ma10 <= 0 or tf_close <= tf_open:
        return None
    chg       = (tf_close - tf_open) / tf_open * 100
    vol_ratio = tf_volume / tf_vol_ma10
    if chg < H4_MTF_MIN_CHG or vol_ratio < H4_MTF_MIN_VOL:
        return None

    result = ScoreResult(symbol=coin.symbol, exchange=coin.exchange)
    result.timeframe   = f"{tf_label}_MTF"
    result.signal_type = f"🕐 {tf_label} MTF PUMP"
    details: list[str] = []

    result.price_chg     = round(chg, 2)
    result.price_current = round(tf_close, 8)
    result.vol_ratio     = round(vol_ratio, 2)
    result.fr            = round(coin.funding_rate * 100, 4)
    result.oi_chg_pct    = round(coin.oi_change_pct, 1)
    result.lsr           = round(coin.lsr, 4)

    # Layer 1: Momentum
    if chg >= 30 and vol_ratio >= 5:
        s1 = 4.0; details.append(f"🚀🚀 {tf_label} bùng nổ (+{chg:.1f}% vol {vol_ratio:.1f}x)")
    elif chg >= 20 and vol_ratio >= 4:
        s1 = 3.5; details.append(f"🚀 {tf_label} rất mạnh (+{chg:.1f}% vol {vol_ratio:.1f}x)")
    elif chg >= 15 and vol_ratio >= 3:
        s1 = 3.0; details.append(f"🚀 {tf_label} mạnh (+{chg:.1f}% vol {vol_ratio:.1f}x)")
    elif chg >= 10 and vol_ratio >= 3:
        s1 = 2.5; details.append(f"📈 {tf_label} tốt (+{chg:.1f}% vol {vol_ratio:.1f}x)")
    else:
        s1 = 2.0; details.append(f"📈 {tf_label} (+{chg:.1f}% vol {vol_ratio:.1f}x)")
    result.total_score += s1

    # Layer 2: Confirm layer (H12 cho H4, H4 cho H2)
    if confirm_green and confirm_vol_r >= 2.0:
        result.total_score += H4_MTF_H12_CONFIRM_BONUS
        details.append(f"✅ Xác nhận nền xanh (vol {confirm_vol_r:.1f}x) +{H4_MTF_H12_CONFIRM_BONUS}đ")
    elif confirm_green:
        result.total_score += H4_MTF_H12_CONFIRM_BONUS * 0.5
        details.append(f"✅ Nền xanh +{H4_MTF_H12_CONFIRM_BONUS*0.5:.1f}đ")
    elif coin.h12_available or coin.h4_available:
        result.total_score -= 0.5
        details.append(f"⚠️ Nền đỏ -- {tf_label} đơn lẻ, cẩn thận")

    # Layer 3: H1 momentum
    if coin.h1_available:
        h1c = coin.h1_price_change_pct
        if h1c >= 8:
            result.total_score += H4_MTF_H1_CONFIRM_BONUS
            details.append(f"⚡ H1 bứt mạnh (+{h1c:.1f}%) +{H4_MTF_H1_CONFIRM_BONUS}đ")
        elif h1c >= 5:
            result.total_score += H4_MTF_H1_CONFIRM_BONUS * 0.7
            details.append(f"⚡ H1 xác nhận (+{h1c:.1f}%)")
        elif h1c >= 2:
            result.total_score += H4_MTF_H1_CONFIRM_BONUS * 0.4
            details.append(f"📈 H1 tích cực (+{h1c:.1f}%)")
        elif h1c < -3:
            details.append(f"⚠️ H1 đang hồi ({h1c:.1f}%)")

    # Layer 4a: Funding Rate
    fr_pct = coin.funding_rate * 100
    if fr_pct <= -0.05:
        result.total_score += H4_MTF_FR_SQUEEZE_BONUS
        details.append(f"💥 FR âm sâu ({fr_pct:.3f}%) +{H4_MTF_FR_SQUEEZE_BONUS}đ")
    elif fr_pct <= -0.01:
        result.total_score += H4_MTF_FR_SQUEEZE_BONUS * 0.5
        details.append(f"💰 FR âm ({fr_pct:.4f}%)")
    elif fr_pct > 0.1:
        result.total_score -= 0.3
        details.append(f"⚠️ FR cao ({fr_pct:.3f}%)")

    # Layer 4b: OI
    oi_c = coin.oi_change_pct
    if oi_c >= 30:
        result.total_score += H4_MTF_OI_EXPAND_BONUS * 1.5
        details.append(f"📡 OI bùng nổ (+{oi_c:.1f}%) +{H4_MTF_OI_EXPAND_BONUS*1.5:.1f}đ")
    elif oi_c >= 15:
        result.total_score += H4_MTF_OI_EXPAND_BONUS
        details.append(f"📡 OI tăng mạnh (+{oi_c:.1f}%)")
    elif oi_c >= 7:
        result.total_score += H4_MTF_OI_EXPAND_BONUS * 0.5
        details.append(f"📡 OI tăng (+{oi_c:.1f}%)")

    # Layer 5: Vol spike bonus
    if vol_ratio >= 6:
        result.total_score += 1.0; details.append(f"🌋 Vol spike cực mạnh ({vol_ratio:.1f}x) +1đ")
    elif vol_ratio >= 4:
        result.total_score += 0.5; details.append(f"📊 Vol spike ({vol_ratio:.1f}x) +0.5đ")

    # Nến đẹp
    body = tf_close - tf_open; wick = tf_high - tf_close
    if body > 0 and wick < body * 0.3:
        result.total_score += 0.3; details.append(f"✅ Nến {tf_label} đẹp")

    result.details     = details
    result.market_mode = f"{tf_label}_MTF"

    if result.total_score < H4_MTF_MIN_SCORE:
        return None

    import math
    tf_range = tf_high - tf_low
    if tf_range > 0:
        def _fmt(v):
            if v <= 0: return 0.0
            digits = max(2, -int(math.floor(math.log10(abs(v)))) + 3)
            return round(v, digits)
        result.entry = _fmt(tf_close)
        result.sl    = _fmt(tf_low - tf_range * 0.1)
        result.tp1   = _fmt(tf_close + tf_range * 0.5)
        result.tp2   = _fmt(tf_close + tf_range * 1.0)
        result.tp3   = _fmt(tf_close + tf_range * 1.618)

    swing_low = tf_low if tf_low > 0 else (coin.low_20d if coin.low_20d > 0 else coin.low)
    calc_dynamic_tp(result, coin, swing_low=swing_low, timeframe=f"{tf_label}_MTF")
    detect_pullback(result, coin, "LONG")
    return result


def score_coin_h4_mtf_pump(coin: CoinData) -> Optional[ScoreResult]:
    """H4 MTF Pump Scanner -- wrapper của _score_mtf_pump_candle."""
    if not coin.h4_available:
        return None
    h12_green = coin.h12_available and coin.h12_close > coin.h12_open
    h12_vol_r = coin.h12_volume / coin.h12_vol_ma5 if coin.h12_vol_ma5 > 0 else 0
    return _score_mtf_pump_candle(
        coin=coin, tf_label="H4",
        tf_open=coin.h4_open, tf_close=coin.h4_close,
        tf_high=coin.h4_high, tf_low=coin.h4_low,
        tf_volume=coin.h4_volume, tf_vol_ma10=coin.h4_vol_ma10,
        confirm_green=h12_green, confirm_vol_r=h12_vol_r,
    )


def score_coin_h2_mtf_pump(coin: CoinData) -> Optional[ScoreResult]:
    """
    H2 MTF Pump Scanner -- bắt pump SỚM HƠN H4 2 giờ.

    Ví dụ HIGH 18/4: H2 close 0.2112 (+45.86% vol 9.1x FR -0.245%)
    alert ngay, thay vì chờ H4 close 0.2445 -- entry rẻ hơn 13.6%.

    Dùng H4 làm confirm layer (H4 xanh = xu hướng lớn hơn ủng hộ).
    Ưu tiên hơn H4 khi cả 2 đều đủ điều kiện (sớm hơn = tốt hơn).
    """
    if not coin.h2_available:
        return None
    h4_green = coin.h4_available and coin.h4_close > coin.h4_open
    h4_vol_r = coin.h4_volume / coin.h4_vol_ma10 if coin.h4_vol_ma10 > 0 else 0
    result = _score_mtf_pump_candle(
        coin=coin, tf_label="H2",
        tf_open=coin.h2_open, tf_close=coin.h2_close,
        tf_high=coin.h2_high, tf_low=coin.h2_low,
        tf_volume=coin.h2_volume, tf_vol_ma10=coin.h2_vol_ma10,
        confirm_green=h4_green, confirm_vol_r=h4_vol_r,
    )
    if result is None:
        return None
    result.timeframe   = "H2_MTF"
    result.signal_type = "⚡ H2 MTF PUMP"
    return result


def score_coin_h4_watchlist(coin: CoinData) -> Optional[ScoreResult]:
    """
    H4 Watch List -- tier thấp hơn H4 MTF, bắt nến bật đáy kiểu PROMPT 19/5.

    Tiêu chí (ít khắt khe hơn H4 MTF):
    -----------------------------------------------------------------
    * H4 tăng 4-9% (thấp hơn MTF 8%, không chồng lấp)
    * Vol 1.8-3.5x MA10 (thấp hơn MTF 3x)
    * OI tăng >= 3% (tiền có vào thật, không phải noise)
    * FR <= 0.05% (không crowded long)
    * H4 xanh (close > open)

    Điểm tích thêm:
    * OI tăng mạnh (>10%) -> +1đ
    * FR âm (squeeze nhỏ) -> +0.5đ
    * H1 cùng chiều (+) -> +0.5đ
    * Giá gần low 20D (bật đáy) -> +1đ

    Output: signal_type = "👀 WATCH LIST"
    -----------------------------------------------------------------
    PROMPT 19/5 23:00 UTC: +6.12%, vol 2.85x, OI tăng, FR +0.005% -> lọt Watch List ✅
    """
    if not ENABLE_H4_WATCHLIST or not coin.h4_available:
        return None

    # Chỉ xét nến xanh
    if coin.h4_close <= coin.h4_open:
        return None

    h4_chg      = coin.h4_price_change_pct
    h4_vol_ratio = coin.h4_volume / coin.h4_vol_ma10 if coin.h4_vol_ma10 > 0 else 0
    fr_pct       = coin.funding_rate * 100
    oi_chg       = coin.oi_change_pct

    # -- Lọc range: không chồng lấp với H4 MTF -------------------
    if h4_chg < H4_WATCH_MIN_CHG:
        return None
    if h4_chg > H4_WATCH_MAX_CHG and h4_vol_ratio >= H4_MTF_MIN_VOL:
        return None   # Đủ điều kiện MTF rồi, không cần Watch List
    if h4_vol_ratio < H4_WATCH_MIN_VOL:
        return None

    # OI phải tăng -- xác nhận có tiền thật vào, không phải pump rác
    if oi_chg < H4_WATCH_OI_MIN:
        return None

    # FR không được crowded long
    if fr_pct > H4_WATCH_FR_MAX:
        return None

    result = ScoreResult(symbol=coin.symbol, exchange=coin.exchange)
    result.timeframe   = "H4_WATCH"
    result.signal_type = "👀 WATCH LIST"
    details = []

    result.price_chg     = round(h4_chg, 2)
    result.price_current = round(coin.h4_close, 8)
    result.vol_ratio     = round(h4_vol_ratio, 2)
    result.fr            = round(fr_pct, 4)
    result.oi_chg_pct    = round(oi_chg, 1)
    result.lsr           = round(coin.lsr, 4)

    # -- Score cơ bản ---------------------------------------------
    if h4_chg >= 7:
        base = 2.0; details.append(f"📈 H4 tốt (+{h4_chg:.1f}% vol {h4_vol_ratio:.1f}x)")
    elif h4_chg >= 5:
        base = 1.5; details.append(f"📈 H4 bật (+{h4_chg:.1f}% vol {h4_vol_ratio:.1f}x)")
    else:
        base = 1.0; details.append(f"📈 H4 nhẹ (+{h4_chg:.1f}% vol {h4_vol_ratio:.1f}x)")
    result.total_score += base

    # Vol bonus
    if h4_vol_ratio >= 2.5:
        result.total_score += 0.5; details.append(f"📊 Vol tốt ({h4_vol_ratio:.1f}x)")

    # -- OI xác nhận ----------------------------------------------
    if oi_chg >= 15:
        result.total_score += 1.0; details.append(f"📡 OI tăng mạnh (+{oi_chg:.1f}%) +1đ")
    elif oi_chg >= 7:
        result.total_score += 0.7; details.append(f"📡 OI tăng (+{oi_chg:.1f}%)")
    elif oi_chg >= 3:
        result.total_score += 0.4; details.append(f"📡 OI tăng nhẹ (+{oi_chg:.1f}%)")

    # -- FR bonus -------------------------------------------------
    if fr_pct <= -0.02:
        result.total_score += 0.5; details.append(f"💥 FR âm ({fr_pct:.4f}%) -- squeeze nhỏ")
    elif -0.02 < fr_pct <= 0.01:
        result.total_score += 0.3; details.append(f"💰 FR neutral ({fr_pct:.4f}%)")

    # -- H1 cùng chiều --------------------------------------------
    if coin.h1_available and coin.h1_price_change_pct >= 2:
        result.total_score += 0.5
        details.append(f"⚡ H1 xanh (+{coin.h1_price_change_pct:.1f}%)")
    elif coin.h1_available and coin.h1_price_change_pct < -3:
        result.total_score -= 0.3
        details.append(f"⚠️ H1 đang kéo lại ({coin.h1_price_change_pct:.1f}%)")

    # -- Bật từ đáy 20D (giá trị nhất với Watch List) -------------
    if coin.low_20d > 0 and coin.h4_close <= coin.low_20d * 1.15:
        result.total_score += 1.0
        pct_above = (coin.h4_close / coin.low_20d - 1) * 100
        details.append(f"🎯 Bật đáy 20D (+{pct_above:.1f}% trên low) +1đ")
    elif coin.low_20d > 0 and coin.h4_close <= coin.low_20d * 1.25:
        result.total_score += 0.5
        details.append(f"📍 Gần đáy 20D")

    if result.total_score < H4_WATCH_MIN_SCORE:
        return None

    # -- Entry / SL / TP ------------------------------------------
    import math
    h4_range = coin.h4_high - coin.h4_low
    if h4_range > 0:
        def _fmt(v):
            if v <= 0: return 0.0
            digits = max(2, -int(math.floor(math.log10(abs(v)))) + 3)
            return round(v, digits)
        result.entry = _fmt(coin.h4_close)
        result.sl    = _fmt(coin.h4_low - h4_range * 0.1)
        # TP theo Fib extension từ đáy H4
        swing_low = coin.h4_low if coin.h4_low > 0 else coin.low
        calc_dynamic_tp(result, coin, swing_low=swing_low, timeframe="H4_WATCH")

    result.details = details
    return result


def score_coin_dump(coin: CoinData) -> Optional[ScoreResult]:
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

    # 2. OI tăng khi giá giảm = thêm short mới vào -> dump tiếp
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
    # OI giảm khi giá giảm = long đang thoát -> dump tiếp
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
        details.append(f"💥 FR dương cao ({fr_pct:.3f}%) -- long trap")
    elif fr_pct >= 0.05:
        result.score_fr = 1.0
        details.append(f"⚠️ FR dương ({fr_pct:.4f}%)")
    elif fr_pct >= 0:
        result.score_fr = 0.5
        details.append(f"💰 FR thấp ({fr_pct:.4f}%)")
    else:
        details.append(f"💰 FR âm ({fr_pct:.4f}%) -- giảm tín hiệu dump")

    # 4. Long/Short Ratio cao = đám đông long = crowded = dễ dump tiếp
    if coin.lsr >= 2.7:
        result.score_lsr = 2.0
        details.append(f"🐂 L/S quá đông long {coin.lsr:.4f} -- dump fuel")
    elif coin.lsr >= MAX_LSR_HEALTHY:
        result.score_lsr = 1.0
        details.append(f"⚠️ L/S crowded {coin.lsr:.4f}")
    elif 0 < coin.lsr <= MAX_LSR_HEALTHY:
        result.score_lsr = -0.5
        details.append(f"📉 L/S healthy {coin.lsr:.4f} -- giảm tín hiệu dump")

    # 5. Liquidation: longs bị liq nhiều hơn -> dump mạnh
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

    # 6. Nến đỏ dài thân (giảm mạnh, bóng trên ngắn) -- xác nhận dump
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
    # Dynamic TP SHORT: Fibonacci extension từ entry -> swing_high (đỉnh 20D)
    swing_high = max(coin.high, coin.low_20d * 1.1) if coin.low_20d > 0 else coin.high
    calc_dynamic_tp(result, coin, swing_high=swing_high, timeframe="1D")
    detect_pullback(result, coin, "SHORT")
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
      Nếu momentum mạnh (h1_chg >= 8%) -> nhân thêm 1.2x cho TP2/TP3
      Nếu vol spike mạnh (h1_vol_ratio >= 3x) -> nhân thêm 1.1x tất cả TP

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

    # Momentum multiplier -- nến càng mạnh thì TP xa hơn
    mom_mult = 1.0
    if h1_chg >= 8:
        mom_mult = 1.3
    elif h1_chg >= 5:
        mom_mult = 1.15
    elif h1_chg >= 3:
        mom_mult = 1.0

    # Volume multiplier -- vol spike xác nhận thêm
    vol_mult = 1.0
    if h1_vol_ratio >= 4:
        vol_mult = 1.2
    elif h1_vol_ratio >= 2:
        vol_mult = 1.1

    # Combined -- chỉ áp dụng cho TP2 và TP3
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

    # REVERSAL là scalp/ngắn hạn nên TP không kéo quá xa kiểu D1 extension.
    # Dùng R-multiple cố định để tránh case TP3 xa bất thường (-30% đến -40%).
    # TP1 ≈ 1R, TP2 ≈ 1.6R, TP3 ≈ 2.3R.
    if result.reversal_type == "DUMP_REVERSAL":
        sl = entry - h1_range * 0.3   # SL dưới đáy nến 1H 30% range
        risk = entry - sl
        if risk <= 0:
            return
        tp1 = entry + risk * 1.0
        tp2 = entry + risk * 1.6
        tp3 = entry + risk * 2.3

        result.sl  = fmt(sl)
        result.tp1 = fmt(tp1)
        result.tp2 = fmt(tp2)
        result.tp3 = fmt(tp3)
        result.rr_tp1 = 1.0
        result.rr_tp2 = 1.6

    else:  # PUMP_REVERSAL / H1_BREAKOUT_SHORT
        sl = entry + h1_range * 0.3   # SL trên đỉnh nến 1H 30% range
        risk = sl - entry
        if risk <= 0:
            return
        tp1 = entry - risk * 1.0
        tp2 = entry - risk * 1.6
        tp3 = entry - risk * 2.3

        # Không cho TP âm đối với coin giá nhỏ.
        tp1 = max(tp1, entry * 0.01)
        tp2 = max(tp2, entry * 0.01)
        tp3 = max(tp3, entry * 0.01)

        result.sl  = fmt(sl)
        result.tp1 = fmt(tp1)
        result.tp2 = fmt(tp2)
        result.tp3 = fmt(tp3)
        result.rr_tp1 = 1.0
        result.rr_tp2 = 1.6


def score_reversal(coin: CoinData) -> Optional[ScoreResult]:
    """
    1H Reversal Engine -- phát hiện 2 loại:
    * PUMP_REVERSAL : coin pump mạnh trên 1D nhưng 1H đang đảo chiều xuống
    * DUMP_REVERSAL : coin dump mạnh trên 1D nhưng 1H đang bật ngược lên

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
    # -> bắt được case MLN pump hôm qua, hôm nay đang đảo chiều
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

    # -- PUMP REVERSAL ---------------------------------------------
    # 1D pump mạnh (trong 3 ngày gần nhất) + 1H đang quay đầu xuống
    is_pump_rev = (
        d1_pump_max >= PUMP_REV_1D_MIN_PUMP
        and h1_chg <= -PUMP_REV_1H_DROP
    )

    # -- DUMP REVERSAL ---------------------------------------------
    # Điều kiện 1: 1D dump mạnh (lookback 3 ngày) + 1H bật ngược
    # Điều kiện 2 (MỚI): Intraday dump sâu trong nến ngày hiện tại + 1H bật ngược
    #   -> bắt case như MLN: open->low dump 34% trong ngày, rồi 1H sau đó bật +10%
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
        details.append(f"🔄 Pump Reversal: 1D +{d1_pump_max:.1f}% ({d1_pump_ref_label}) -> 1H {h1_chg:.1f}%")

        # Điểm theo độ mạnh của đảo chiều 1H
        if h1_chg <= -8:
            score += 3.0; details.append(f"📉 1H drop cực mạnh ({h1_chg:.1f}%)")
        elif h1_chg <= -5:
            score += 2.0; details.append(f"📉 1H drop mạnh ({h1_chg:.1f}%)")
        else:
            score += 1.0; details.append(f"📉 1H drop ({h1_chg:.1f}%)")

        # 1D pump càng cao -> đà bán lại càng mạnh
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
            score += 1.5; details.append(f"💥 FR dương cao ({fr_pct:.3f}%) -- long trap")
        elif fr_pct >= 0.05:
            score += 0.5; details.append(f"⚠️ FR dương ({fr_pct:.4f}%)")
        # FR âm sâu sau pump = shorts vào quyết liệt, xác nhận đảo chiều
        elif fr_pct <= -0.20:
            score += 2.0; details.append(f"💥 FR âm sâu ({fr_pct:.3f}%) -- shorts cực quyết")
        elif fr_pct <= -0.10:
            score += 1.0; details.append(f"⚠️ FR âm ({fr_pct:.4f}%) -- shorts giữ mạnh")

        # OI giảm khi 1H đỏ = long đang thoát
        if coin.oi_change_pct <= -5:
            score += 1.5; details.append(f"📡 OI giảm ({coin.oi_change_pct:.1f}%) -- long thoát")
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
            result.signal_type = "🔄💥 PUMP REV -- BÁN RẤT MẠNH"
        elif score >= 6:
            result.signal_type = "🔄💥 PUMP REV -- BÁN MẠNH"
        elif score >= 4:
            result.signal_type = "🔄📉 PUMP REVERSAL"
        else:
            result.signal_type = "🔄 Pump -> Quay Đầu"

    else:  # DUMP_REVERSAL
        result.reversal_type = "DUMP_REVERSAL"
        result.market_mode   = "DUMP_REVERSAL"

        # Xác định nguồn gốc dump để hiển thị đúng
        if is_dump_rev_intraday and not is_dump_rev_lookback:
            # Intraday dump -- dùng intraday_dump_pct làm đại diện
            effective_dump = -coin.intraday_dump_pct
            dump_source    = f"intraday open->low"
        else:
            effective_dump = d1_dump_max
            dump_source    = d1_dump_ref_label

        details.append(f"🔄 Dump Reversal: {effective_dump:.1f}% ({dump_source}) -> 1H +{h1_chg:.1f}%")

        # Điểm theo độ mạnh bật ngược 1H
        if h1_chg >= 8:
            score += 3.0; details.append(f"🚀 1H bật cực mạnh (+{h1_chg:.1f}%)")
        elif h1_chg >= 5:
            score += 2.0; details.append(f"🚀 1H bật mạnh (+{h1_chg:.1f}%)")
        else:
            score += 1.0; details.append(f"📈 1H bật (+{h1_chg:.1f}%)")

        # Độ sâu dump -- ưu tiên intraday nếu sâu hơn
        dump_depth = coin.intraday_dump_pct if coin.intraday_dump_pct >= abs(d1_dump_max) else abs(d1_dump_max)
        if dump_depth >= 30:
            score += 3.0; details.append(f"📉 Dump cực sâu (-{dump_depth:.1f}%) -- nảy mạnh")
        elif dump_depth >= 20:
            score += 2.0; details.append(f"📉 Dump rất sâu (-{dump_depth:.1f}%)")
        elif dump_depth >= 12:
            score += 1.5; details.append(f"📉 Dump sâu (-{dump_depth:.1f}%)")
        elif dump_depth >= 8:
            score += 1.0; details.append(f"📉 Dump (-{dump_depth:.1f}%)")

        # Bonus thêm nếu là intraday dump -- wick dài = "lau sàn" xong bật
        if is_dump_rev_intraday and coin.intraday_dump_pct >= INTRADAY_DUMP_MIN:
            lower_wick_pct = coin.intraday_dump_pct
            if lower_wick_pct >= 25:
                score += 1.5; details.append(f"🕯️ Wick dài cực mạnh (-{lower_wick_pct:.1f}%) -- lau sàn xong bật")
            elif lower_wick_pct >= 15:
                score += 1.0; details.append(f"🕯️ Wick dài (-{lower_wick_pct:.1f}%) -- tín hiệu đáy tạm")

        # Vol 1H tăng khi bật = xác nhận mua vào
        if h1_vol_ratio >= DUMP_REV_1H_VOL_MULT * 2:
            score += 2.0; details.append(f"💥 Vol 1H spike mạnh ({h1_vol_ratio:.1f}x)")
        elif h1_vol_ratio >= DUMP_REV_1H_VOL_MULT:
            score += 1.0; details.append(f"📊 Vol 1H tăng ({h1_vol_ratio:.1f}x)")

        # FR âm sâu sau dump = short squeeze tiềm năng
        if fr_pct <= -0.10:
            score += 1.5; details.append(f"💥 FR âm sâu ({fr_pct:.3f}%) -- short squeeze")
        elif fr_pct <= -0.05:
            score += 0.5; details.append(f"💰 FR âm ({fr_pct:.4f}%)")

        # OI tăng khi 1H xanh = short mới vào = short squeeze fuel
        if coin.oi_change_pct >= 10:
            score += 1.5; details.append(f"📡 OI tăng ({coin.oi_change_pct:.1f}%) -- short squeeze setup")
        elif coin.oi_change_pct >= 5:
            score += 0.5; details.append(f"📡 OI tăng nhẹ ({coin.oi_change_pct:.1f}%)")

        # Liq: longs bị clear trong dump -> sạch nhiên liệu để bật
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
            result.signal_type = "🔄💥 DUMP REV -- MUA RẤT MẠNH"
        elif score >= 6:
            result.signal_type = "🔄💥 DUMP REV -- MUA MẠNH"
        elif score >= 4:
            result.signal_type = "🔄📈 DUMP REVERSAL"
        else:
            result.signal_type = "🔄 Dump -> Bật Ngược"

    result.total_score = round(score, 1)
    result.details     = details

    if result.total_score < MIN_REVERSAL_SCORE:
        return None

    # -- M30 Confirmation -----------------------------------------
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
                result.details.append(f"✅ M30 xác nhận: 2 nến xanh ({m30_prev_chg:+.1f}% -> {m30_chg:+.1f}%)")
            elif m30_chg > 0:
                result.total_score += 0.8
                result.m30_confirmed = True
                result.details.append(f"✅ M30 xác nhận: nến xanh ({m30_chg:+.1f}%)")
            elif m30_chg < -2:
                # M30 đỏ ngược chiều = cảnh báo, trừ điểm
                result.total_score -= 1.0
                result.details.append(f"⚠️ M30 ngược chiều ({m30_chg:.1f}%) -- chờ xác nhận")
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
                result.details.append(f"✅ M30 xác nhận: 2 nến đỏ ({m30_prev_chg:+.1f}% -> {m30_chg:+.1f}%)")
            elif m30_chg < 0:
                result.total_score += 0.8
                result.m30_confirmed = True
                result.details.append(f"✅ M30 xác nhận: nến đỏ ({m30_chg:+.1f}%)")
            elif m30_chg > 2:
                result.total_score -= 1.0
                result.details.append(f"⚠️ M30 ngược chiều ({m30_chg:+.1f}%) -- chờ xác nhận")
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

    # Phát hiện lực hồi để quyết định Entry Now hay Entry Limit
    rev_side = "LONG" if result.reversal_type == "DUMP_REVERSAL" else "SHORT"
    detect_pullback(result, coin, rev_side)

    return result


# ==============================================================
# SCANNER
# ==============================================================

def score_h1_breakout(coin: CoinData) -> Optional[ScoreResult]:
    """
    1H Momentum Breakout -- signal độc lập với 1D.

    Điều kiện cần:
      * h1_available = True
      * |h1_chg| >= H1_BREAKOUT_MIN_CHG (8%)
      * h1_vol_ratio >= H1_BREAKOUT_MIN_VOL (5x)

    2 chiều:
      * H1_BREAKOUT_LONG  : h1_chg >= +8%, vol >= 5x -> pump mạnh, có thể long
      * H1_BREAKOUT_SHORT : h1_chg <= -8%, vol >= 5x -> dump mạnh, có thể short

    Bổ sung điểm từ:
      * FR âm sâu (short squeeze fuel cho long)
      * FR dương cao (long trap -> xác nhận short)
      * OI tăng cùng chiều
      * M30 xác nhận
      * Liq cùng chiều
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

        # FR âm -> shorts đang trả phí -> squeeze fuel -> cộng điểm
        if fr_pct <= -0.5:
            score += 3.0; details.append(f"🔴 FR âm cực sâu ({fr_pct:.3f}%) -- short squeeze")
        elif fr_pct <= -0.2:
            score += 2.0; details.append(f"💥 FR âm sâu ({fr_pct:.3f}%) -- squeeze fuel")
        elif fr_pct <= H1_BREAKOUT_FR_BONUS:
            score += 1.0; details.append(f"💰 FR âm ({fr_pct:.4f}%)")

        # OI tăng khi giá tăng = long mới vào = momentum thật
        if coin.oi_change_pct >= 15:
            score += 1.5; details.append(f"📡 OI tăng mạnh (+{coin.oi_change_pct:.1f}%) -- long vào")
        elif coin.oi_change_pct >= 5:
            score += 0.5; details.append(f"📡 OI tăng ({coin.oi_change_pct:.1f}%)")
        # OI giảm khi giá tăng = short đang cover = cũng tốt
        elif coin.oi_change_pct <= -5:
            score += 1.0; details.append(f"📡 OI giảm ({coin.oi_change_pct:.1f}%) -- short cover")

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
            result.signal_type = "🚀💥 H1 BREAKOUT LONG -- CỰC MẠNH"
        elif score >= 7:
            result.signal_type = "🚀 H1 BREAKOUT LONG -- RẤT MẠNH"
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
            score += 2.0; details.append(f"💥 FR dương cao ({fr_pct:.3f}%) -- long trap")
        elif fr_pct >= 0.05:
            score += 1.0; details.append(f"⚠️ FR dương ({fr_pct:.4f}%)")
        # FR âm khi dump = shorts không tin tưởng, dump có thể đảo
        elif fr_pct <= -0.2:
            score -= 0.5; details.append(f"⚠️ FR âm ({fr_pct:.3f}%) -- dump yếu hơn")

        # OI tăng khi giá giảm = short mới vào = dump tiếp
        if coin.oi_change_pct >= 15:
            score += 1.5; details.append(f"📡 OI tăng ({coin.oi_change_pct:.1f}%) -- short vào")
        elif coin.oi_change_pct >= 5:
            score += 0.5; details.append(f"📡 OI tăng nhẹ ({coin.oi_change_pct:.1f}%)")

        # Liq: longs bị liq = cascade
        if coin.liq_longs > 0:
            lx = coin.liq_longs / max(coin.liq_shorts, 1)
            if lx >= 5:
                score += 2.0; details.append(f"💥 Longs liq {lx:.0f}x -- cascade")
            elif lx >= 2:
                score += 1.0; details.append(f"✅ Longs liq {lx:.1f}x")

        # Nến 1H đỏ thân dài
        h1_body  = abs(coin.h1_close - coin.h1_open)
        h1_upper = coin.h1_high - coin.h1_open
        if h1_body > 0 and h1_upper < h1_body * 0.3:
            score += 0.5; details.append("✅ Nến 1H đỏ thân dài")

        # Signal label
        if score >= 10:
            result.signal_type = "📉💥 H1 BREAKOUT SHORT -- CỰC MẠNH"
        elif score >= 7:
            result.signal_type = "📉 H1 BREAKOUT SHORT -- RẤT MẠNH"
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

    # Tính TP/SL -- dùng lại calc_reversal_tp với map reversal_type
    if is_long:
        result.reversal_type = "DUMP_REVERSAL"   # map tạm để dùng LONG formula
        calc_reversal_tp(result, coin)
        result.reversal_type = "H1_BREAKOUT_LONG"
        detect_pullback(result, coin, "LONG")
    else:
        result.reversal_type = "PUMP_REVERSAL"   # map tạm để dùng SHORT formula
        calc_reversal_tp(result, coin)
        result.reversal_type = "H1_BREAKOUT_SHORT"
        detect_pullback(result, coin, "SHORT")

    return result


def calc_h2_tp_sl(result: ScoreResult, h2_high: float, h2_low: float,
                   h2_close: float, direction: str) -> None:
    """
    TP/SL ngắn hạn cho H2 -- target 15-30%.
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
    Score coin khung H2 -- tìm pump/dump ngắn hạn sau khi nến H2 đóng.
    Ngưỡng thấp hơn 1D: MIN_CHG=7%, MIN_VOL=1.3x, MIN_SCORE=4.0đ
    Target: TP 15-30%

    Điểm đặc biệt so với D:
    - Thêm bonus nến không bóng dưới (low=open) -> mua mạnh
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
            score += 2.0; details.append(f"📡 OI +{oi_chg:.1f}% -- long mới vào mạnh")
        elif oi_chg >= 10:
            score += 1.0; details.append(f"📡 OI +{oi_chg:.1f}%")
        elif oi_chg <= -5:
            score += 1.0; details.append(f"📡 OI -{abs(oi_chg):.1f}% -- short cover")

        # 4. FR âm = short squeeze fuel
        if fr_pct <= -0.3:
            score += 2.0; details.append(f"💥 FR âm sâu ({fr_pct:.3f}%) -- short squeeze")
        elif fr_pct <= -0.05:
            score += 1.0; details.append(f"💰 FR âm ({fr_pct:.4f}%)")
        elif fr_pct <= 0.05:
            score += 0.5; details.append(f"💰 FR thấp ({fr_pct:.4f}%)")

        # 5. Liq: shorts bị liq >> longs
        if liq_longs > 0:
            liq_ratio = liq_shorts / liq_longs
            if liq_ratio >= 5:
                score += 2.0; details.append(f"💥 Shorts liq {liq_ratio:.0f}x -- squeeze mạnh")
            elif liq_ratio >= 2:
                score += 1.0; details.append(f"✅ Shorts liq {liq_ratio:.1f}x")
        elif liq_shorts > 0:
            score += 1.5; details.append(f"💥 Shorts liq -- không có long bị liq")

        # 6. Nến đặc biệt: low=open = không có selling pressure
        if abs(h2_l - h2_o) / h2_o < 0.002:   # low ≈ open (trong 0.2%)
            score += 1.0; details.append("🕯️ Low≈Open -- không có bóng dưới (mua mạnh)")

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
            score += 1.0; details.append(f"📡 OI +{oi_chg:.1f}% -- short vào")
        elif oi_chg <= -10:
            score += 1.5; details.append(f"📡 OI -{abs(oi_chg):.1f}% -- long tháo")

        if fr_pct >= 0.15:
            score += 1.5; details.append(f"💥 FR dương cao ({fr_pct:.3f}%) -- long trap")
        elif fr_pct >= 0.05:
            score += 0.5

        if liq_longs > 0 and liq_shorts > 0:
            lr = liq_longs / liq_shorts
            if lr >= 5:
                score += 2.0; details.append(f"💥 Longs liq {lr:.0f}x -- cascade")
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



def score_distribution_short(coin: CoinData) -> Optional[ScoreResult]:
    """Institutional SHORT engine: bắt blowoff top -> distribution -> post-squeeze dump.

    Dùng để bắt các case kiểu BILL / MLN:
    - Nến D/H6 dump mạnh sau pump
    - Râu trên lớn, failed continuation
    - OI rollover / long liquidation
    - Funding âm nhưng giá vẫn rơi = negative-funding trap
    """
    if not ENABLE_DISTRIBUTION_ENGINE:
        return None

    h6 = get_ohlcv_h6(coin.exchange, coin.symbol, limit=4)
    h12 = get_ohlcv_h12(coin.exchange, coin.symbol, limit=3)
    if not h6 or len(h6) < 2:
        return None

    h6_last = h6[-1]
    h6_o = float(h6_last.get("o", 0))
    h6_h = float(h6_last.get("h", 0))
    h6_l = float(h6_last.get("l", 0))
    h6_c = float(h6_last.get("c", 0))
    if h6_o <= 0 or h6_c <= 0 or h6_h <= h6_l:
        return None

    h6_chg = (h6_c - h6_o) / h6_o * 100
    h6_range = h6_h - h6_l
    h6_upper_wick = h6_h - max(h6_o, h6_c)
    h6_body = abs(h6_c - h6_o)

    h12_chg = 0.0
    if h12 and len(h12) >= 1:
        h12_last = h12[-1]
        h12_o = float(h12_last.get("o", 0))
        h12_c = float(h12_last.get("c", 0))
        if h12_o > 0:
            h12_chg = (h12_c - h12_o) / h12_o * 100

    result = ScoreResult(symbol=coin.symbol, exchange=coin.exchange)
    result.timeframe = "H6/D"
    result.price_current = round(coin.close, 8)
    result.price_chg = round(coin.price_change_pct, 2)
    result.oi_chg_pct = round(coin.oi_change_pct, 1)
    result.fr = round(coin.funding_rate * 100, 4)
    result.lsr = round(coin.lsr, 4)
    result.vol_ratio = round((coin.volume / coin.vol_ma10), 2) if coin.vol_ma10 > 0 else 0

    score = 0.0
    details = []

    # 1) H6 breakdown sau blowoff
    if h6_chg <= -15:
        score += 3.0
        details.append(f"H6 dump mạnh {h6_chg:.1f}%")
    elif h6_chg <= -H6_BREAKDOWN_MIN_DROP:
        score += 2.0
        details.append(f"H6 breakdown {h6_chg:.1f}%")

    # 2) H12 cũng đỏ = momentum short có xác nhận MTF
    if h12_chg <= -H12_BREAKDOWN_MIN_DROP:
        score += 2.0
        details.append(f"H12 breakdown {h12_chg:.1f}%")
    elif h12_chg <= -5:
        score += 1.0
        details.append(f"H12 yếu {h12_chg:.1f}%")

    # 3) Daily blowoff / failed continuation: râu trên lớn hoặc ngày đang dump mạnh
    d_range = coin.high - coin.low
    if d_range > 0:
        d_upper = coin.high - max(coin.open, coin.close)
        upper_ratio = d_upper / d_range
        if upper_ratio >= DAILY_BLOWOFF_UPPER_WICK_RATIO and coin.price_change_pct < 0:
            score += 2.0
            details.append("Daily failed continuation")
        elif coin.price_change_pct <= -12:
            score += 2.0
            details.append(f"Daily dump {coin.price_change_pct:.1f}%")
        elif coin.price_change_pct <= -7:
            score += 1.0
            details.append(f"Daily yếu {coin.price_change_pct:.1f}%")

    # 4) OI rollover / longs bị unwind
    if coin.oi_change_pct <= -10:
        score += 2.0
        details.append(f"OI rollover {coin.oi_change_pct:.1f}%")
    elif coin.oi_change_pct <= OI_ROLLOVER_MIN_PCT:
        score += 1.0
        details.append(f"OI giảm {coin.oi_change_pct:.1f}%")

    # 5) Funding âm nhưng giá vẫn rơi = không còn squeeze continuation, dễ là trap long
    fr_pct = coin.funding_rate * 100
    if fr_pct < -0.01 and (h6_chg < 0 or coin.price_change_pct < 0):
        score += 1.0
        details.append(f"FR âm trap {fr_pct:.3f}%")
    elif fr_pct > 0.05 and (h6_chg < 0 or coin.price_change_pct < 0):
        score += 1.0
        details.append(f"Long crowded FR {fr_pct:.3f}%")

    # 6) Volume xác nhận panic/distribution
    if coin.vol_ma10 > 0:
        vr = coin.volume / coin.vol_ma10
        if vr >= 3 and coin.price_change_pct < 0:
            score += 2.0
            details.append(f"Panic volume {vr:.1f}x")
        elif vr >= 1.5 and coin.price_change_pct < 0:
            score += 1.0
            details.append(f"Volume xác nhận {vr:.1f}x")

    # 7) Nến H6 reject / râu trên lớn
    if h6_body > 0 and h6_upper_wick > h6_body * 1.2:
        score += 1.0
        details.append("H6 rejection wick")

    # 8) Liquidation: longs bị liquidate nhiều hơn shorts
    if coin.liq_longs > 0 and coin.liq_shorts > 0:
        long_liq_ratio = coin.liq_longs / max(coin.liq_shorts, 1)
        if long_liq_ratio >= 3:
            score += 1.5
            details.append(f"Long liq {long_liq_ratio:.1f}x")
        elif long_liq_ratio >= 1.5:
            score += 0.8
            details.append(f"Long liq {long_liq_ratio:.1f}x")

    result.total_score = round(score, 1)
    if result.total_score < MIN_DISTRIBUTION_SCORE:
        return None

    result.signal_type = "KHUYẾN NGHỊ SHORT"
    result.market_mode = "DISTRIBUTION_SHORT"
    result.reversal_type = "DISTRIBUTION_SHORT"
    result.details = details

    # ===== Entry/SL/TP cụ thể =====
    # Entry short tốt nhất là dead-cat bounce về 38.2-61.8% của nến breakdown H6.
    # Nếu range H6 quá nhỏ thì fallback sang daily range.
    swing_high = h6_h
    swing_low = h6_l
    if (swing_high - swing_low) / max(h6_c, 1e-12) < 0.03 and d_range > 0:
        swing_high = coin.high
        swing_low = coin.low

    move = swing_high - swing_low
    entry_low = swing_low + move * DEADCAT_RETRACE_MIN
    entry_high = swing_low + move * DEADCAT_RETRACE_MAX
    entry_mid = (entry_low + entry_high) / 2

    # SL trên swing high, thêm buffer 1.5%
    sl = swing_high * 1.015

    # TP theo liquidity dưới đáy breakdown
    tp1 = swing_low * 0.985
    tp2 = swing_low * 0.94
    tp3 = swing_low * 0.88

    result.entry = _smart_round(entry_mid)
    result.sl = _smart_round(sl)
    result.tp1 = _smart_round(tp1)
    result.tp2 = _smart_round(tp2)
    result.tp3 = _smart_round(tp3)

    risk = abs(result.sl - result.entry)
    if risk > 0:
        result.rr_tp1 = round(abs(result.entry - result.tp1) / risk, 2)
        result.rr_tp2 = round(abs(result.entry - result.tp2) / risk, 2)

    return result


def _smart_round(v: float) -> float:
    """Round giá theo số chữ số phù hợp với coin nhỏ/lớn."""
    import math
    if v <= 0:
        return 0.0
    digits = max(2, -int(math.floor(math.log10(abs(v)))) + 4)
    return round(v, digits)

def scan_one_symbol(exchange: str, symbol: str) -> tuple[
    Optional[ScoreResult], Optional[ScoreResult], Optional[ScoreResult],
    Optional[ScoreResult], Optional[ScoreResult], Optional[ScoreResult]
]:
    """Scan 1 coin. Trả về (pump, dump, reversal, h4_mtf, h2_mtf, h4_watch)."""
    coin = fetch_coin_data(exchange, symbol)
    if coin is None:
        return None, None, None, None, None, None
    pump = score_coin_pump(coin)
    dump = score_coin_dump(coin)
    dist = score_distribution_short(coin)
    if dist and (dump is None or dist.total_score >= dump.total_score):
        dump = dist
    # H4 MTF và H2 MTF -- H2 ưu tiên hơn khi cả 2 đều có (sớm hơn 2 giờ)
    h4_mtf = score_coin_h4_mtf_pump(coin) if ENABLE_H4_MTF_SCAN else None
    h2_mtf = score_coin_h2_mtf_pump(coin) if ENABLE_H4_MTF_SCAN else None
    # Nếu H2 đủ điều kiện thì bỏ H4 (tránh alert 2 lần cùng coin)
    if h2_mtf and h4_mtf:
        h4_mtf = None
    # Watch List
    h4_watch = score_coin_h4_watchlist(coin) if ENABLE_H4_WATCHLIST else None
    if (h4_mtf or h2_mtf) and h4_watch:
        h4_watch = None
    # Reversal tạm tắt
    reversal = None
    return pump, dump, reversal, h4_mtf, h2_mtf, h4_watch


def run_scan_exchange(exchange: str) -> tuple[list[ScoreResult], list[ScoreResult], list[ScoreResult]]:
    log.info("=" * 60)
    log.info(f"🔍 FAST SCAN {exchange} -- {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    log.info("=" * 60)

    symbols = get_all_symbols(exchange)
    if not symbols:
        log.error(f"Không lấy được danh sách symbols {exchange}!")
        return [], [], [], [], [], []

    pump_results:  list[ScoreResult] = []
    dump_results:  list[ScoreResult] = []
    rev_results:   list[ScoreResult] = []
    h4_results:    list[ScoreResult] = []
    h2_results:    list[ScoreResult] = []
    watch_results: list[ScoreResult] = []
    errors = 0
    workers = MAX_WORKERS_BINANCE if exchange == "Binance" else MAX_WORKERS_BINGX if exchange == "BingX" else MAX_WORKERS_KUCOIN if exchange == "KuCoin" else MAX_WORKERS_BYBIT

    log.info(f"🚀 {exchange}: scanning {len(symbols)} symbols với {workers} workers...")

    if not FAST_SCAN:
        for i, symbol in enumerate(symbols, 1):
            try:
                if i == 1 or i % LOG_EVERY_N == 0 or i == len(symbols):
                    log.info(f"[{exchange} {i}/{len(symbols)}] Scanning...")
                pump, dump, rev, h4, h2, watch = scan_one_symbol(exchange, symbol)
                if pump:  pump_results.append(pump)
                if dump:  dump_results.append(dump)
                if rev:   rev_results.append(rev)
                if h4:    h4_results.append(h4)
                if h2:    h2_results.append(h2)
                if watch: watch_results.append(watch)
            except Exception as e:
                errors += 1
                log.warning(f"  ❌ {exchange} {symbol}: {e}")
        for lst in (pump_results, dump_results, rev_results, h4_results, h2_results, watch_results):
            lst.sort(key=lambda x: x.total_score, reverse=True)
        log.info(f"✅ {exchange} scan xong | pump {len(pump_results)} | dump {len(dump_results)} | h4 {len(h4_results)} | h2 {len(h2_results)} | watch {len(watch_results)}")
        return pump_results, dump_results, rev_results, h4_results, h2_results, watch_results

    completed = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {executor.submit(scan_one_symbol, exchange, s): s for s in symbols}
        for future in as_completed(future_map):
            symbol = future_map[future]
            completed += 1
            try:
                pump, dump, rev, h4, h2, watch = future.result()
                if pump:  pump_results.append(pump);  log.info(f"  ✅ PUMP  {pump.display_symbol}: {pump.total_score:.1f}đ")
                if dump:  dump_results.append(dump);  log.info(f"  ✅ DUMP  {dump.display_symbol}: {dump.total_score:.1f}đ")
                if h4:    h4_results.append(h4);      log.info(f"  🕐 H4   {h4.display_symbol}: {h4.total_score:.1f}đ +{h4.price_chg:.1f}% vol {h4.vol_ratio:.1f}x")
                if h2:    h2_results.append(h2);      log.info(f"  ⚡ H2   {h2.display_symbol}: {h2.total_score:.1f}đ +{h2.price_chg:.1f}% vol {h2.vol_ratio:.1f}x")
                if watch: watch_results.append(watch);log.info(f"  👀 WATCH {watch.display_symbol}: {watch.total_score:.1f}đ")
            except Exception as e:
                errors += 1
                log.debug(f"  ❌ {exchange} {symbol}: {e}")
            if completed == 1 or completed % LOG_EVERY_N == 0 or completed == len(symbols):
                log.info(f"[{exchange}] {completed}/{len(symbols)} | h4 {len(h4_results)} | h2 {len(h2_results)} | watch {len(watch_results)} | err {errors}")

    for lst in (pump_results, dump_results, rev_results, h4_results, h2_results, watch_results):
        lst.sort(key=lambda x: x.total_score, reverse=True)
    log.info(f"✅ {exchange} FAST scan xong | h4 {len(h4_results)} | h2 {len(h2_results)} | watch {len(watch_results)} | errors {errors}")
    return pump_results, dump_results, rev_results, h4_results, h2_results, watch_results

def run_scan() -> tuple[list[ScoreResult], list[ScoreResult], list[ScoreResult], list[ScoreResult], list[ScoreResult], list[ScoreResult]]:
    """Quét 2 sàn SONG SONG. Trả về (pump, dump, rev, h4, h2, watch)."""
    TOP_PUMP = 2; TOP_DUMP = 2

    all_pump: list[ScoreResult]  = []
    all_dump: list[ScoreResult]  = []
    all_rev:  list[ScoreResult]  = []
    all_h4:   list[ScoreResult]  = []
    all_h2:   list[ScoreResult]  = []
    all_watch:list[ScoreResult]  = []

    scan_start = time.time()
    log.info(f"🚀 Parallel scan bắt đầu: {len(SCAN_EXCHANGES)} sàn...")

    with ThreadPoolExecutor(max_workers=MAX_WORKERS_EXCHANGES) as ex_pool:
        future_map = {ex_pool.submit(run_scan_exchange, ex): ex for ex in SCAN_EXCHANGES}
        for future in as_completed(future_map):
            exchange = future_map[future]
            try:
                pump_ex, dump_ex, rev_ex, h4_ex, h2_ex, watch_ex = future.result()
                if PER_EXCHANGE_TOP_N:
                    all_pump.extend(pump_ex[:TOP_N_FINAL]); all_dump.extend(dump_ex[:TOP_N_FINAL])
                    all_h4.extend(h4_ex[:TOP_N_FINAL]);     all_h2.extend(h2_ex[:TOP_N_FINAL])
                    all_watch.extend(watch_ex[:TOP_N_FINAL])
                else:
                    all_pump.extend(pump_ex); all_dump.extend(dump_ex); all_rev.extend(rev_ex)
                    all_h4.extend(h4_ex);    all_h2.extend(h2_ex);     all_watch.extend(watch_ex)
                log.info(f"✅ {exchange}: pump {len(pump_ex)} | dump {len(dump_ex)} | h4 {len(h4_ex)} | h2 {len(h2_ex)} | watch {len(watch_ex)} | {time.time()-scan_start:.0f}s")
            except Exception as e:
                log.error(f"❌ {exchange} scan error: {e}", exc_info=True)

    log.info(f"⏱️ Scan hoàn tất {time.time()-scan_start:.1f}s")

    def dedup(lst: list[ScoreResult]) -> list[ScoreResult]:
        seen: dict[str, ScoreResult] = {}
        for r in sorted(lst, key=lambda x: x.total_score, reverse=True):
            base = r.symbol.upper().rstrip("M") if r.symbol.upper().endswith("USDTM") else r.symbol.upper()
            if base not in seen: seen[base] = r
        return list(seen.values())

    unique_pump  = dedup(all_pump);  unique_dump  = dedup(all_dump)
    unique_rev   = dedup(all_rev);   unique_h4    = dedup(all_h4)
    unique_h2    = dedup(all_h2);    unique_watch = dedup(all_watch)

    # H2 ưu tiên: nếu coin vào H2 rồi thì bỏ khỏi H4
    h2_syms = {r.symbol.upper().rstrip("M") for r in unique_h2}
    unique_h4    = [r for r in unique_h4    if r.symbol.upper().rstrip("M") not in h2_syms]
    # Watch List: loại coin đã vào H2 hoặc H4
    mtf_syms = h2_syms | {r.symbol.upper().rstrip("M") for r in unique_h4}
    unique_watch = [r for r in unique_watch if r.symbol.upper().rstrip("M") not in mtf_syms]

    # PUMP
    squeezes = [r for r in unique_pump if r.market_mode in ("SQUEEZE","HYBRID") or r.squeeze_engine_score >= SQUEEZE_MIN_SCORE]
    squeezes.sort(key=lambda x: (x.squeeze_engine_score, x.total_score), reverse=True)
    final_pump: list[ScoreResult] = []
    if squeezes:
        top_sq = squeezes[0]; final_pump.append(top_sq)
        remaining = [r for r in unique_pump if r.symbol.upper().rstrip("M") != top_sq.symbol.upper().rstrip("M")]
    else:
        remaining = unique_pump
    remaining.sort(key=lambda x: x.total_score, reverse=True)
    final_pump.extend(remaining); final_pump = final_pump[:TOP_PUMP]

    unique_dump.sort(key=lambda x: x.total_score, reverse=True)
    final_dump = unique_dump[:TOP_DUMP]
    final_rev  = select_top_reversal_long_short(unique_rev)

    unique_h2.sort(key=lambda x: x.total_score, reverse=True)
    unique_h4.sort(key=lambda x: x.total_score, reverse=True)
    unique_watch.sort(key=lambda x: x.total_score, reverse=True)

    return final_pump, final_dump, final_rev, unique_h4[:3], unique_h2[:3], unique_watch[:H4_WATCH_MAX_COINS]
    """
    Quét 2 sàn SONG SONG, gộp kết quả, trả về (pump_top, dump_top, reversal_top).

    Kiến trúc parallel 2 tầng:
      Tầng 1: 2 sàn chạy đồng thời (ThreadPoolExecutor MAX_WORKERS_EXCHANGES=2)
      Tầng 2: Mỗi sàn scan symbol của mình song song (workers riêng từng sàn)

    Quy tắc PUMP: SQUEEZE ưu tiên TOP 1, còn lại theo total_score.
    Quy tắc DUMP: top 2 theo total_score.
    Quy tắc REVERSAL: chỉ lấy 1 LONG + 1 SHORT, ưu tiên Binance/Bybit.
    """
    TOP_PUMP = 2
    TOP_DUMP = 2

    all_pump: list[ScoreResult] = []
    all_dump: list[ScoreResult] = []
    all_rev:  list[ScoreResult] = []
    all_h4:   list[ScoreResult] = []

    scan_start = time.time()
    log.info(f"🚀 Parallel scan bắt đầu: {len(SCAN_EXCHANGES)} sàn đồng thời...")

    # Tầng 1: 3 sàn chạy song song
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_EXCHANGES) as ex_pool:
        future_map = {
            ex_pool.submit(run_scan_exchange, exchange): exchange
            for exchange in SCAN_EXCHANGES
        }
        for future in as_completed(future_map):
            exchange = future_map[future]
            try:
                pump_ex, dump_ex, rev_ex, h4_ex = future.result()
                if PER_EXCHANGE_TOP_N:
                    all_pump.extend(pump_ex[:TOP_N_FINAL])
                    all_dump.extend(dump_ex[:TOP_N_FINAL])
                    all_rev.extend(rev_ex[:TOP_N_FINAL])
                    all_h4.extend(h4_ex[:TOP_N_FINAL])
                else:
                    all_pump.extend(pump_ex)
                    all_dump.extend(dump_ex)
                    all_rev.extend(rev_ex)
                    all_h4.extend(h4_ex)
                log.info(
                    f"✅ {exchange} xong: "
                    f"pump {len(pump_ex)} | dump {len(dump_ex)} | rev {len(rev_ex)} | h4 {len(h4_ex)} "
                    f"| elapsed {time.time()-scan_start:.0f}s"
                )
            except Exception as e:
                log.error(f"❌ {exchange} scan error: {e}", exc_info=True)

    log.info(f"⏱️ Parallel scan hoàn tất trong {time.time()-scan_start:.1f}s")
    log.info(f"   Tổng trước dedup: pump {len(all_pump)} | dump {len(all_dump)} | rev {len(all_rev)} | h4 {len(all_h4)}")

    def dedup(lst: list[ScoreResult]) -> list[ScoreResult]:
        """Cùng symbol giữ bản có điểm cao nhất."""
        seen: dict[str, ScoreResult] = {}
        for r in sorted(lst, key=lambda x: x.total_score, reverse=True):
            base = r.symbol.upper()
            if base.endswith("USDTM"):
                base = base[:-1]
            if base not in seen:
                seen[base] = r
        return list(seen.values())

    unique_pump = dedup(all_pump)
    unique_dump = dedup(all_dump)
    unique_rev  = dedup(all_rev)
    unique_h4   = dedup(all_h4)

    # -- PUMP: SQUEEZE ưu tiên TOP 1 ------------------------------
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

    # -- DUMP: theo total_score -------------------------------------
    unique_dump.sort(key=lambda x: x.total_score, reverse=True)
    final_dump = unique_dump[:TOP_DUMP]

    # -- REVERSAL ----------------------------------------------------
    final_rev = select_top_reversal_long_short(unique_rev)

    # -- H4 MTF: top 3 điểm cao nhất --------------------------------
    unique_h4.sort(key=lambda x: x.total_score, reverse=True)
    final_h4 = unique_h4[:3]

    return final_pump, final_dump, final_rev, final_h4


# ==============================================================
# TELEGRAM ALERT
# ==============================================================

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
                 rev_results: list[ScoreResult],
                 h4_results: list[ScoreResult] | None = None,
                 h2_results: list[ScoreResult] | None = None,
                 watch_results: list[ScoreResult] | None = None) -> str:
    """Telegram alert gọn: chỉ hiện section nào có signal."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"🚀📉 <b>PUMP &amp; DUMP SCANNER V7</b>",
        f"🕒 <b>{now}</b>",
        f"📊 Quét: {' | '.join(SCAN_EXCHANGES)} -- 1D + H4 + 1H\n",
    ]

    def fmt_price(v: float) -> str:
        return f"{v:.6g}" if v and v > 0 else "-"

    def pct(t: float, e: float) -> str:
        if e <= 0:
            return ""
        return f"{(t - e) / e * 100:+.2f}%"

    def entry_block(r: ScoreResult, side: str) -> str:
        """Tách Entry Now / Entry Limit dựa trên lực hồi.

        Logic mới:
        -----------------------------------------------------------------
        SHORT:
          * Có lực hồi (has_pullback=True, pullback_type=RETRACING):
            -> Entry Now: KHÔNG (đang hồi lên, chờ)
            -> Entry Limit: dùng limit_entry_fib (vùng hồi 38.2%) hoặc r.entry

          * Không có lực hồi (CONTINUING):
            -> Entry Now: giá hiện tại (momentum dump đang tiếp diễn)
            -> Bỏ Entry Limit (đã entry now rồi)

        LONG:
          * Có lực hồi nhỏ (M30 kéo lại):
            -> Entry Now: giá hiện tại (có thể bắt ngay nếu muốn)
            -> Entry Limit: vùng hồi 23.6%-38.2% thấp hơn (vào rẻ hơn)

          * Không có lực hồi (đang bật mạnh):
            -> Entry Now: giá hiện tại
            -> Bỏ Entry Limit
        -----------------------------------------------------------------
        """
        current     = r.price_current or 0
        limit_entry = r.entry or current

        # Dist giữa giá hiện tại và limit entry
        dist_pct = abs(current - limit_entry) / limit_entry * 100 if limit_entry > 0 else 0

        sl_txt = f"SL: <b>{fmt_price(r.sl)}</b>"

        if side == "SHORT":
            if r.has_pullback and r.pullback_type == "RETRACING":
                # Đang hồi lên -> chỉ hiện Entry Limit (chờ hồi xong mới short)
                pullback_note = f"+{r.pullback_pct:.1f}%" if r.pullback_pct > 0 else ""
                fib_limit = r.limit_entry_fib if r.limit_entry_fib > 0 else limit_entry
                return (
                    f"⏳ Đang hồi {pullback_note} -- chờ short\n"
                    f"🎯 Entry Limit: <b>{fmt_price(fib_limit)}</b> | {sl_txt}"
                )
            else:
                # Dump tiếp tục -> chỉ hiện Entry Now, không có Limit
                return (
                    f"⚡ Entry Now: <b>{fmt_price(current)}</b> | {sl_txt}"
                )

        else:  # LONG
            if r.has_pullback and r.pullback_type == "RETRACING":
                # M30 đang kéo lại -> chỉ hiện Entry Limit (chờ giá về vùng rẻ hơn)
                fib_limit = r.limit_entry_fib if r.limit_entry_fib > 0 else limit_entry
                return (
                    f"⏳ M30 đang kéo lại -- chờ long\n"
                    f"🎯 Entry Limit: <b>{fmt_price(fib_limit)}</b> | {sl_txt}"
                )
            else:
                # Bật mạnh -> chỉ hiện Entry Now, không có Limit
                if dist_pct > 3.0:
                    return f"⚡ Entry Now: <b>Không chase</b> | {sl_txt}"
                return f"⚡ Entry Now: <b>{fmt_price(current)}</b> | {sl_txt}"

    # -- PUMP SECTION: chỉ hiện khi có kết quả -------------------
    if pump_results:
        lines.append("===========================")
        lines.append("🚀 <b>TOP PUMP -- CÓ THỂ TĂNG MẠNH (1D)</b>")
        lines.append("===========================\n")

        pump_rank_styles = [
            ("🟢🥇", "TOP 1 PUMP -- ƯU TIÊN MẠNH"),
            ("🟡🥈", "TOP 2 PUMP -- THEO DÕI"),
        ]

        for i, r in enumerate(pump_results[:2]):
            badge, rank_name = pump_rank_styles[i] if i < len(pump_rank_styles) else ("⭐", "WATCHLIST")
            symbol = html.escape(r.display_symbol)
            engine_info = ""
            if r.market_mode in ("SQUEEZE", "HYBRID"):
                engine_info = f" | 🔴SQ:{r.squeeze_engine_score:.1f}"
            elif r.market_mode == "TREND":
                engine_info = f" | 🟢TR:{r.trend_score:.1f}"

            lines.append(
                f"{badge} <b>{rank_name}</b>\n"
                f"<b>{symbol}</b> -- <b>{r.total_score:.1f}đ</b>{engine_info}\n"
                f"🟢 <b>KHUYẾN NGHỊ LONG</b>\n"
                f"💰 Giá: <b>{fmt_price(r.price_current)}</b> | +{r.price_chg:.2f}%"
            )
            if r.entry > 0 and r.tp1 > 0:
                lines.append(
                    f"{entry_block(r, 'LONG')}\n"
                    f"1️⃣ TP1: <b>{fmt_price(r.tp1)}</b> ({pct(r.tp1, r.entry)})\n"
                    f"2️⃣ TP2: <b>{fmt_price(r.tp2)}</b> ({pct(r.tp2, r.entry)})\n"
                    f"3️⃣ TP3: <b>{fmt_price(r.tp3)}</b> ({pct(r.tp3, r.entry)})"
                )
            lines.append("")

    else:
        lines.append("===========================")
        lines.append("🚀 <b>TOP PUMP -- CÓ THỂ TĂNG MẠNH (1D)</b>")
        lines.append("===========================\n")
        lines.append("💤 <i>Chưa có coin pump đủ điều kiện trong khung này.</i>\n")

    # -- DUMP SECTION: chỉ hiện khi có kết quả -------------------
    if dump_results:
        lines.append("===========================")
        lines.append("📉 <b>TOP DUMP -- CÓ THỂ GIẢM MẠNH (1D)</b>")
        lines.append("===========================\n")

        dump_rank_styles = [
            ("🔴🥇", "TOP 1 DUMP -- CẨN THẬN CAO"),
            ("🟠🥈", "TOP 2 DUMP -- THEO DÕI"),
        ]

        for i, r in enumerate(dump_results[:2]):
            badge, rank_name = dump_rank_styles[i] if i < len(dump_rank_styles) else ("⭐", "WATCHLIST")
            symbol = html.escape(r.display_symbol)

            lines.append(
                f"{badge} <b>{rank_name}</b>\n"
                f"<b>{symbol}</b> -- <b>{r.total_score:.1f}đ</b>\n"
                f"🔻 <b>KHUYẾN NGHỊ SHORT</b>\n"
                f"💰 Giá: <b>{fmt_price(r.price_current)}</b> | {r.price_chg:.2f}%"
            )
            if r.entry > 0 and r.tp1 > 0:
                lines.append(
                    f"{entry_block(r, 'SHORT')}\n"
                    f"1️⃣ TP1: <b>{fmt_price(r.tp1)}</b> ({pct(r.tp1, r.entry)})\n"
                    f"2️⃣ TP2: <b>{fmt_price(r.tp2)}</b> ({pct(r.tp2, r.entry)})\n"
                    f"3️⃣ TP3: <b>{fmt_price(r.tp3)}</b> ({pct(r.tp3, r.entry)})"
                )
            lines.append("")

    else:
        lines.append("===========================")
        lines.append("📉 <b>TOP DUMP -- CÓ THỂ GIẢM MẠNH (1D)</b>")
        lines.append("===========================\n")
        lines.append("💤 <i>Chưa có coin dump đủ điều kiện trong khung này.</i>\n")

    # -- REVERSAL SECTION: tạm tắt -------------------------------
    # if rev_results: ...

    # -- H4 MTF PUMP SECTION --------------------------------------
    if h4_results:
        lines.append("===========================")
        lines.append("🕐 <b>H4 MTF PUMP -- BẮT SỚM TRONG NGÀY</b>")
        lines.append("===========================\n")

        for i, r in enumerate(h4_results[:3]):
            symbol = html.escape(r.display_symbol)
            rank = ["🥇", "🥈", "🥉"][i] if i < 3 else "⭐"
            lines.append(
                f"{rank} <b>{symbol}</b> -- <b>{r.total_score:.1f}đ</b>\n"
                f"🟢 <b>KHUYẾN NGHỊ LONG (H4)</b>\n"
                f"💰 H4: <b>{fmt_price(r.price_current)}</b> | +{r.price_chg:.2f}% | Vol {r.vol_ratio:.1f}x"
            )
            if r.entry > 0 and r.tp1 > 0:
                lines.append(
                    f"{entry_block(r, 'LONG')}\n"
                    f"1️⃣ TP1: <b>{fmt_price(r.tp1)}</b> ({pct(r.tp1, r.entry)})\n"
                    f"2️⃣ TP2: <b>{fmt_price(r.tp2)}</b> ({pct(r.tp2, r.entry)})\n"
                    f"3️⃣ TP3: <b>{fmt_price(r.tp3)}</b> ({pct(r.tp3, r.entry)})"
                )
            lines.append("")

    # -- H2 MTF PUMP SECTION -- entry sớm hơn H4 2 giờ -------------
    if h2_results:
        lines.append("===========================")
        lines.append("⚡ <b>H2 MTF PUMP -- ENTRY SỚM NHẤT</b>")
        lines.append("===========================\n")

        for i, r in enumerate(h2_results[:3]):
            symbol = html.escape(r.display_symbol)
            rank = ["🥇", "🥈", "🥉"][i] if i < 3 else "⭐"
            lines.append(
                f"{rank} <b>{symbol}</b> -- <b>{r.total_score:.1f}đ</b>\n"
                f"🟢 <b>KHUYẾN NGHỊ LONG (H2)</b>\n"
                f"💰 H2: <b>{fmt_price(r.price_current)}</b> | +{r.price_chg:.2f}% | Vol {r.vol_ratio:.1f}x"
            )
            if r.entry > 0 and r.tp1 > 0:
                lines.append(
                    f"{entry_block(r, 'LONG')}\n"
                    f"1️⃣ TP1: <b>{fmt_price(r.tp1)}</b> ({pct(r.tp1, r.entry)})\n"
                    f"2️⃣ TP2: <b>{fmt_price(r.tp2)}</b> ({pct(r.tp2, r.entry)})\n"
                    f"3️⃣ TP3: <b>{fmt_price(r.tp3)}</b> ({pct(r.tp3, r.entry)})"
                )
            lines.append("")

    # -- WATCH LIST SECTION ----------------------------------------
    if watch_results:
        lines.append("===========================")
        lines.append("👀 <b>WATCH LIST -- THEO DÕI, CHƯA VÀO</b>")
        lines.append("===========================")
        lines.append("<i>Bật đáy nhẹ, OI tăng -- chờ xác nhận thêm</i>\n")

        for r in watch_results[:H4_WATCH_MAX_COINS]:
            symbol = html.escape(r.display_symbol)
            oi_tag = f" OI +{r.oi_chg_pct:.1f}%" if r.oi_chg_pct > 0 else ""
            fr_tag = f" FR {r.fr:+.4f}%" if r.fr != 0 else ""
            lines.append(
                f"👀 <b>{symbol}</b> -- {r.total_score:.1f}đ\n"
                f"📈 H4: <b>{fmt_price(r.price_current)}</b> | +{r.price_chg:.2f}% | Vol {r.vol_ratio:.1f}x{oi_tag}{fr_tag}"
            )
            if r.entry > 0 and r.sl > 0:
                sl_dist = abs(r.entry - r.sl) / r.entry * 100
                lines.append(
                    f"⚡ Entry nếu xác nhận: <b>{fmt_price(r.entry)}</b>\n"
                    f"🛑 SL: <b>{fmt_price(r.sl)}</b> (cách {sl_dist:.1f}%)"
                )
                if r.tp1 > 0:
                    lines.append(
                        f"1️⃣ TP1: <b>{fmt_price(r.tp1)}</b> ({pct(r.tp1, r.entry)})  "
                        f"3️⃣ TP3: <b>{fmt_price(r.tp3)}</b> ({pct(r.tp3, r.entry)})"
                    )
            lines.append("")

    lines.append("⚠️ <i>Không phải lời khuyên đầu tư. Luôn đặt SL.</i>")
    return "\n".join(lines)
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


# ==============================================================
# MAIN JOB
# ==============================================================

def run_reversal_scan() -> list[ScoreResult]:
    """
    Chỉ scan Reversal (1H) -- dùng cho job 30 phút.
    3 sàn chạy SONG SONG, dedup, trả về top 1 LONG + top 1 SHORT.
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

    # 3 sàn song song
    with ThreadPoolExecutor(max_workers=MAX_WORKERS_EXCHANGES) as ex_pool:
        futures = {ex_pool.submit(_scan_exchange_reversal, ex): ex for ex in SCAN_EXCHANGES}
        for future in as_completed(futures):
            try:
                all_rev.extend(future.result())
            except Exception as e:
                log.error(f"❌ Reversal scan error {futures[future]}: {e}")

    log.info(f"⏱️ Reversal parallel scan xong: {time.time()-scan_start:.1f}s | {len(all_rev)} total")

    # Dedup -- KuCoin USDTM -> strip M trước khi so sánh
    seen: dict[str, ScoreResult] = {}
    for r in sorted(all_rev, key=lambda x: x.total_score, reverse=True):
        base = r.symbol.upper()
        if base.endswith("USDTM"):
            base = base[:-1]
        if base not in seen:
            seen[base] = r
    unique = list(seen.values())

    # Chỉ gửi 1 LONG + 1 SHORT điểm cao nhất, ưu tiên Binance/Bybit.
    return select_top_reversal_long_short(unique)


def _reversal_side(r: ScoreResult) -> str:
    """Map reversal signal về LONG/SHORT để lọc top theo từng phía."""
    if r.reversal_type in ("DUMP_REVERSAL", "H1_BREAKOUT_LONG"):
        return "LONG"
    if r.reversal_type in ("PUMP_REVERSAL", "H1_BREAKOUT_SHORT", "DISTRIBUTION_SHORT"):
        return "SHORT"
    # fallback theo signal_type nếu có custom signal mới
    sig = (r.signal_type or "").upper()
    if "LONG" in sig:
        return "LONG"
    if "SHORT" in sig or "DUMP" in sig:
        return "SHORT"
    return ""


def _reversal_rank_key(r: ScoreResult) -> tuple[float, int, float]:
    """Rank REVERSAL: điểm chính + ưu tiên Binance/Bybit + volume ratio."""
    ex_bonus = REVERSAL_PRIORITY_EXCHANGES.get(r.exchange, 0)
    effective_score = r.total_score + (REVERSAL_PRIORITY_SCORE_BONUS if ex_bonus >= 2 else 0)
    return (effective_score, ex_bonus, r.vol_ratio)


def select_top_reversal_long_short(results: list[ScoreResult]) -> list[ScoreResult]:
    """
    Trả về tối đa 4 signal REVERSAL:
    - top 2 LONG điểm cao nhất
    - top 2 SHORT điểm cao nhất
    Ưu tiên Binance/Bybit khi điểm gần nhau nhờ priority bonus nhỏ.
    """
    selected: list[ScoreResult] = []
    for side in ("LONG", "SHORT"):
        side_items = [r for r in results if _reversal_side(r) == side]
        if not side_items:
            continue
        top_n = sorted(side_items, key=_reversal_rank_key, reverse=True)[:REVERSAL_TOP_PER_SIDE]
        selected.extend(top_n)
    selected.sort(key=_reversal_rank_key, reverse=True)
    return selected


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
    """Alert REVERSAL ngắn gọn: coin, LONG/SHORT, entry, SL, TP."""
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"🔄 <b>REVERSAL ALERT -- {now}</b>",
        f"📊 {' | '.join(SCAN_EXCHANGES)} -- 1H + M30 | Top 1 LONG + Top 1 SHORT\n",
    ]

    if not rev_results:
        lines.append("<i>Không có reversal đủ điều kiện.</i>")
        return "\n".join(lines)

    for r in rev_results:
        symbol = html.escape(r.display_symbol)
        is_long = r.reversal_type in ("DUMP_REVERSAL", "H1_BREAKOUT_LONG")
        side_line = "🟢 <b>KHUYẾN NGHỊ LONG</b>" if is_long else "🔻 <b>KHUYẾN NGHỊ SHORT</b>"

        def pct(target: float, entry: float) -> str:
            if entry <= 0:
                return ""
            return f"{(target - entry) / entry * 100:+.2f}%"

        lines.append(
            f"🔄 <b>{symbol}</b> -- <b>{r.total_score:.1f}đ</b>\n"
            f"{side_line}\n"
            f"💰 Giá: <b>{r.price_current:.6g}</b> | 1H: {r.h1_chg:+.2f}% | M30: {r.m30_chg:+.2f}%"
        )
        if r.entry > 0 and r.tp1 > 0:
            sl_txt = f"SL: <b>{r.sl:.6g}</b>"
            if r.has_pullback and r.pullback_type == "RETRACING":
                pb_dir  = f"+{r.pullback_pct:.1f}%" if not is_long else f"-{r.pullback_pct:.1f}%"
                fib_e   = r.limit_entry_fib if r.limit_entry_fib > 0 else r.entry
                fib_tag = " (Fib 38.2%)" if fib_e != r.entry else ""
                entry_line = (
                    f"⏳ Đang hồi {pb_dir} -- chờ {'long' if is_long else 'short'}\n"
                    f"🎯 Entry Limit: <b>{fib_e:.6g}</b>{fib_tag} | {sl_txt}"
                )
            else:
                entry_line = f"⚡ Entry Now: <b>{r.entry:.6g}</b> | {sl_txt}"

            lines.append(
                f"{entry_line}\n"
                f"1️⃣ TP1: <b>{r.tp1:.6g}</b> ({pct(r.tp1, r.entry)})\n"
                f"2️⃣ TP2: <b>{r.tp2:.6g}</b> ({pct(r.tp2, r.entry)})\n"
                f"3️⃣ TP3: <b>{r.tp3:.6g}</b> ({pct(r.tp3, r.entry)})"
            )
        lines.append("")

    lines.append("⚠️ <i>Không phải lời khuyên đầu tư. Luôn đặt SL.</i>")
    return "\n".join(lines)


def job_reversal_only():
    """Job chạy lúc xx:32 UTC -- chỉ scan và gửi Reversal nếu có signal."""
    try:
        rev_results = run_reversal_scan()

        if not rev_results:
            log.info("🔄 Reversal scan xong -- không có signal mới.")
            return  # Không gửi Telegram nếu không có gì

        log.info(f"🔄 Reversal scan xong -- {len(rev_results)} signal(s):")
        for r in rev_results:
            log.info(f"  {r.reversal_type}: {r.display_symbol} {r.total_score:.1f}đ -- {r.signal_type}")
            log.info(f"  1D: {r.price_chg:+.2f}% | 1H: {r.h1_chg:+.2f}%")

        register_active_trades(rev_results, source="REVERSAL_30M")
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
    # Institutional / SMC-style score
    inst_score: float = 0
    final_score: float = 0
    bias: str = "NEUTRAL"
    trade_quality: str = "NO TRADE"
    inst_notes: list = field(default_factory=list)
    signal_type: str   = ""
    direction:   str   = "PUMP"

    @property
    def display_symbol(self) -> str:
        s = self.symbol
        if s.endswith("USDTM"): return s[:-5]
        if s.endswith("USDT"):  return s[:-4]
        return s



def _pct_change(o: float, c: float) -> float:
    return (c - o) / o * 100 if o > 0 else 0.0


def _wick_profile(o: float, h: float, l: float, c: float) -> tuple[float, float, float]:
    rng = max(h - l, 1e-12)
    body = abs(c - o)
    upper = h - max(o, c)
    lower = min(o, c) - l
    return body / rng, upper / rng, lower / rng


def _institutional_mtf_score(result: MTFResult, candles_d: list, candles_h12: Optional[list], candles_h6: Optional[list],
                             d_vr: float, funding_rate: float = 0.0, oi_change_pct: float = 0.0,
                             lsr: float = 0.0, liq_longs: float = 0.0, liq_shorts: float = 0.0) -> None:
    """
    Institutional / SMC-style daily selector.
    Mục tiêu: chọn coin có setup giống phân tích thủ công:
    - Structure đa khung rõ (D/H12/H6 cùng bias)
    - Compression -> Expansion
    - Sweep/reclaim hoặc breakout acceptance
    - Futures sạch: funding chưa crowded, OI xác nhận, L/S không quá lệch
    - Né trap: wick xả, funding nóng, OI tăng nhưng giá không chạy
    """
    notes: list[str] = []
    score = 0.0
    direction = result.direction

    d = candles_d[-1]
    d_o, d_h, d_l, d_c = map(lambda k: float(d.get(k, 0)), ["o", "h", "l", "c"])
    body_pct, upper_wick_pct, lower_wick_pct = _wick_profile(d_o, d_h, d_l, d_c)
    d_chg = _pct_change(d_o, d_c)

    # -- 1. Market Structure D/H12/H6 ---------------------------
    if result.green_count == 3:
        score += 2.0
        notes.append("MTF 3/3 cùng chiều")
    elif result.green_count == 2:
        score += 1.0
        notes.append("MTF 2/3 cùng chiều")

    # Higher-low / lower-high proxy từ 5 nến D gần nhất
    if len(candles_d) >= 6:
        lows = [float(x.get("l", 0)) for x in candles_d[-6:]]
        highs = [float(x.get("h", 0)) for x in candles_d[-6:]]
        if direction == "PUMP":
            if lows[-1] >= min(lows[-4:-1]) and d_c > sum(float(x.get("c", 0)) for x in candles_d[-4:-1]) / 3:
                score += 1.0; notes.append("HL/reclaim structure")
            if d_c > max(highs[-4:-1]):
                score += 1.2; notes.append("BOS breakout D")
        else:
            if highs[-1] <= max(highs[-4:-1]) and d_c < sum(float(x.get("c", 0)) for x in candles_d[-4:-1]) / 3:
                score += 1.0; notes.append("LH/reject structure")
            if d_c < min(lows[-4:-1]):
                score += 1.2; notes.append("BOS breakdown D")

    # -- 2. Liquidity sweep / reclaim proxy ---------------------
    if len(candles_d) >= 4:
        prev_lows = [float(x.get("l", 0)) for x in candles_d[-4:-1]]
        prev_highs = [float(x.get("h", 0)) for x in candles_d[-4:-1]]
        prev_low = min(prev_lows); prev_high = max(prev_highs)
        if direction == "PUMP" and d_l < prev_low and d_c > prev_low and lower_wick_pct >= 0.35:
            score += 2.0; notes.append("sell-side sweep + reclaim")
        if direction == "DUMP" and d_h > prev_high and d_c < prev_high and upper_wick_pct >= 0.35:
            score += 2.0; notes.append("buy-side sweep + rejection")

    # -- 3. Compression -> Expansion ----------------------------
    if len(candles_d) >= 12:
        ranges = [float(x.get("h", 0)) - float(x.get("l", 0)) for x in candles_d[-11:-1]]
        avg_rng = sum(ranges) / len(ranges) if ranges else 0
        cur_rng = d_h - d_l
        if avg_rng > 0 and cur_rng > avg_rng * 1.35 and d_vr >= 1.2:
            score += 1.2; notes.append("compression -> expansion")
        elif avg_rng > 0 and cur_rng < avg_rng * 0.75:
            score -= 0.8; notes.append("compression chưa break")

    # -- 4. Candle / absorption trap filter ---------------------
    if direction == "PUMP":
        if upper_wick_pct > 0.45 and body_pct < 0.35:
            score -= 2.0; notes.append("wick xả mạnh / bull trap")
        elif body_pct >= 0.45 and upper_wick_pct <= 0.30:
            score += 0.8; notes.append("nến acceptance đẹp")
    else:
        if lower_wick_pct > 0.45 and body_pct < 0.35:
            score -= 2.0; notes.append("wick hấp thụ / bear trap")
        elif body_pct >= 0.45 and lower_wick_pct <= 0.30:
            score += 0.8; notes.append("nến breakdown đẹp")

    # -- 5. Futures data: OI / Funding / LSR / Liquidation ------
    fr_pct = funding_rate * 100
    liq_ratio = (liq_shorts / liq_longs) if liq_longs > 0 else (99.0 if liq_shorts > 0 else 0.0)

    if direction == "PUMP":
        if oi_change_pct >= 15 and d_chg > 0:
            score += 1.0; notes.append(f"OI xác nhận +{oi_change_pct:.1f}%")
        elif oi_change_pct >= 15 and d_chg <= 1:
            score -= 1.2; notes.append("OI tăng nhưng giá không chạy")
        if fr_pct <= 0.03:
            score += 0.8; notes.append("funding sạch")
        elif fr_pct >= 0.10:
            score -= 1.0; notes.append("funding long crowded")
        if 0 < lsr <= 2.3:
            score += 0.5; notes.append("L/S healthy")
        elif lsr > 2.8:
            score -= 1.0; notes.append("L/S crowded long")
        if liq_ratio >= 1.5:
            score += 0.8; notes.append("short squeeze fuel")
    else:
        if oi_change_pct >= 15 and d_chg < 0:
            score += 1.0; notes.append(f"OI short xác nhận +{oi_change_pct:.1f}%")
        elif oi_change_pct >= 15 and d_chg >= -1:
            score -= 1.2; notes.append("OI tăng nhưng breakdown yếu")
        if fr_pct >= 0.05:
            score += 0.8; notes.append("funding thuận short / long trap")
        elif fr_pct <= -0.10:
            score -= 1.0; notes.append("funding âm dễ short squeeze")
        if lsr > 2.3:
            score += 0.5; notes.append("long crowded")
        if liq_longs > liq_shorts * 1.5:
            score += 0.8; notes.append("long squeeze fuel")

    # -- 6. Final quality label ---------------------------------
    result.inst_score = round(score, 2)
    result.final_score = round(result.mtf_score + result.inst_score, 2)
    result.inst_notes = notes[:6]

    if result.final_score >= 6.5 and result.inst_score >= 3:
        result.trade_quality = "A+ INSTITUTIONAL"
    elif result.final_score >= 4.5 and result.inst_score >= 1.5:
        result.trade_quality = "A SETUP"
    elif result.final_score >= 3.0:
        result.trade_quality = "B SETUP / WAIT CONFIRM"
    else:
        result.trade_quality = "NO TRADE / LOW EDGE"

    if direction == "PUMP":
        result.bias = "STRONG LONG" if result.final_score >= 6.5 else "LONG" if result.final_score >= 4.5 else "NEUTRAL"
    else:
        result.bias = "STRONG SHORT" if result.final_score >= 6.5 else "SHORT" if result.final_score >= 4.5 else "NEUTRAL"

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

    # -- H12 ------------------------------------------------------
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

    # -- H6 -------------------------------------------------------
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

    # -- MTF score: D=50%, H12=20%, H6=30% ------------------------
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

    # -- TP / SL từ range nến D ------------------------------------
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

    # -- Institutional Futures + SMC score -----------------------
    fr = get_funding_rate(exchange, symbol) or 0.0
    oi_change = 0.0
    oi_hist = get_oi_history(exchange, symbol, limit=6)
    if oi_hist and len(oi_hist) >= 5:
        oi_now = float(oi_hist[-1].get("openInterest", 0))
        oi_old = float(oi_hist[-5].get("openInterest", 0))
        if oi_old > 0:
            oi_change = (oi_now - oi_old) / oi_old * 100
    lsr = get_lsr(exchange, symbol) or 0.0
    liq_longs, liq_shorts = get_liquidation(exchange, symbol)
    _institutional_mtf_score(result, candles_d, candles_h12 if 'candles_h12' in locals() else None,
                             candles_h6 if 'candles_h6' in locals() else None, d_vr, fr, oi_change, lsr, liq_longs, liq_shorts)

    # Filter cuối: bỏ setup thiếu edge institutional rõ ràng
    if result.final_score < 3.0 or result.trade_quality.startswith("NO TRADE"):
        return None

    # Signal label
    gc = result.green_count
    prefix = "🏦 " + result.trade_quality + " -- "
    if gc == 3:
        result.signal_type = prefix + ("3/3 KHUNG LONG" if result.direction == "PUMP" else "3/3 KHUNG SHORT")
    else:
        result.signal_type = prefix + ("2/3 KHUNG LONG" if result.direction == "PUMP" else "2/3 KHUNG SHORT")

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

    # 3 sàn song song
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
        for r in sorted(lst, key=lambda x: (x.green_count, abs(x.final_score)), reverse=True):
            base = r.symbol.upper()
            if base.endswith("USDTM"): base = base[:-1]
            if base not in seen:
                seen[base] = r
        return list(seen.values())

    unique_pump = dedup_mtf(all_pump)
    unique_dump = dedup_mtf(all_dump)

    # Sort pump: green_count cao -> mtf_score cao
    unique_pump.sort(key=lambda x: (x.green_count, x.final_score), reverse=True)
    # Sort dump: green_count cao -> mtf_score âm nhất (abs cao nhất) = dump mạnh nhất
    unique_dump.sort(key=lambda x: (x.green_count, abs(x.final_score)), reverse=True)

    log.info(f"📅 MTF scan xong: {time.time()-scan_start:.1f}s | pump {len(unique_pump)} | dump {len(unique_dump)}")
    return unique_pump[:1], unique_dump[:1]


def format_mtf_alert(pump_list: list[MTFResult], dump_list: list[MTFResult]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [f"📅 <b>DAILY MTF SCAN -- {now}</b>\n"]

    def fmt_coin(r: MTFResult, section_icon: str, section_label: str) -> None:
        sym = html.escape(r.display_symbol)
        def pct(t, e):
            if e <= 0: return ""
            return f"{(t-e)/e*100:+.2f}%"
        lines.append(f"{'='*27}")
        lines.append(f"{section_icon} <b>{section_label}</b>")
        lines.append(f"{'='*27}\n")
        notes = ", ".join(html.escape(x) for x in getattr(r, "inst_notes", [])[:5])
        lines.append(
            f"<b>{sym}</b> -- <b>{r.final_score:.2f}đ</b> | MTF {r.mtf_score:.2f} + INST {r.inst_score:.2f} | {r.green_count}/3 khung\n"
            f"BIAS: <b>{html.escape(r.bias)}</b> | <b>{html.escape(r.trade_quality)}</b>\n"
            f"⚡ <b>{html.escape(r.signal_type)}</b>\n"
            f"D: {r.icon_d}{r.chg_d:+.1f}% | H12: {r.icon_h12}{r.chg_h12:+.1f}% | H6: {r.icon_h6}{r.chg_h6:+.1f}%\n"
            f"Vol D: {r.vr_d:.1f}x | Vol H6: {r.vr_h6:.1f}x\n"
            f"SMC/Futures: {notes if notes else 'đợi xác nhận thêm'}"
        )
        if r.entry > 0 and r.tp1 > 0:
            lines.append(
                f"📍 Entry: <b>{r.entry:.6g}</b> | SL: <b>{r.sl:.6g}</b>\n"
                f"1️⃣ TP1: <b>{r.tp1:.6g}</b> ({pct(r.tp1, r.entry)})\n"
                f"2️⃣ TP2: <b>{r.tp2:.6g}</b> ({pct(r.tp2, r.entry)})\n"
                f"3️⃣ TP3: <b>{r.tp3:.6g}</b> ({pct(r.tp3, r.entry)})"
            )
        lines.append("")

    if pump_list:
        fmt_coin(pump_list[0], "🚀", "TOP MUA NGÀY HÔM NAY")
    else:
        lines.append(f"{'='*27}\n🚀 <b>TOP MUA NGÀY HÔM NAY</b>\n{'='*27}\n")
        lines.append("<i>Không có signal pump đủ 2/3 khung.</i>\n")

    if dump_list:
        fmt_coin(dump_list[0], "📉", "TOP SHORT/DUMP NGÀY HÔM NAY")
    else:
        lines.append(f"{'='*27}\n📉 <b>TOP SHORT/DUMP NGÀY HÔM NAY</b>\n{'='*27}\n")
        lines.append("<i>Không có signal dump đủ 2/3 khung.</i>\n")

    lines.append("⚠️ <i>Không phải lời khuyên đầu tư. Luôn đặt SL.</i>")
    return "\n".join(lines)


# -- daily_watch.json -- track coin để HOLD/OUT check ----------

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
                  "mtf_score": r.mtf_score, "final_score": r.final_score, "inst_score": r.inst_score, "bias": r.bias, "green_count": r.green_count} for r in pump],
        "dump": [{"symbol": r.symbol, "exchange": r.exchange,
                  "entry": r.entry, "sl": r.sl,
                  "tp1": r.tp1, "tp2": r.tp2, "tp3": r.tp3,
                  "d_high": r.d_high, "d_low": r.d_low,
                  "mtf_score": r.mtf_score, "final_score": r.final_score, "inst_score": r.inst_score, "bias": r.bias, "green_count": r.green_count} for r in dump],
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
    """00:02 UTC -- Quét MTF, alert top 2 pump + dump, lưu watch list."""
    try:
        log.info("📅 Daily MTF scan bắt đầu...")
        pump, dump = run_mtf_scan()

        if not pump and not dump:
            log.info("📅 Không có MTF signal đủ điều kiện.")
            send_telegram("📅 Daily MTF scan xong -- không có signal đủ 2/4 khung.")
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
    04:02, 08:02, 12:02, 16:02, 20:02 UTC -- Check HOLD/OUT cho coin đã alert.
    Dựa trên nến H4 mới nhất.
    """
    watch = load_daily_watch()
    if not watch or (not watch.get("pump") and not watch.get("dump")):
        log.info("🔍 Hold check: không có coin trong watch list.")
        return

    today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    if watch.get("date") != today:
        log.info(f"🔍 Hold check: watch list ngày {watch.get('date')} ≠ hôm nay {today} -- bỏ qua.")
        return

    now_str = datetime.now(timezone.utc).strftime("%H:%M UTC")
    lines   = [f"🔍 <b>HOLD/OUT CHECK -- {now_str}</b>\n"]
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

        # TP/SL check -- dùng H6 high/low để bắt TP chính xác hơn
        check_price_high = h6_h if direction == "PUMP" else h6_l
        check_price_low  = h6_l if direction == "PUMP" else h6_h

        def pct(t, e): return f"{(t-e)/e*100:+.2f}%" if e > 0 else ""

        # Xác định verdict
        if direction == "PUMP":
            if check_price_high >= tp3 and tp3 > 0:
                verdict = f"🎯🎯🎯 TP3 HIT! ({pct(tp3, entry)})"
                urgent  = True
            elif check_price_high >= tp2 and tp2 > 0:
                verdict = f"🎯🎯 TP2 HIT! ({pct(tp2, entry)}) -- Cân nhắc chốt"
                urgent  = True
            elif check_price_high >= tp1 and tp1 > 0:
                verdict = f"🎯 TP1 HIT ({pct(tp1, entry)}) -- Di SL lên entry"
                urgent  = True
            elif check_price_low <= sl:
                verdict = f"🛑 SL HIT ({pct(sl, entry)})"
                urgent  = True
            elif h6_chg <= -3 and h6_vr >= 1.5:
                verdict = f"⚠️ OUT -- H6 đỏ {h6_chg:.1f}% vol {h6_vr:.1f}x"
                urgent  = True
            elif h6_chg >= 2 and h6_vr >= 1.2:
                verdict = f"✅ HOLD -- H6 {h6_chg:+.1f}% vol {h6_vr:.1f}x"
                urgent  = False
            else:
                verdict = f"✅ HOLD -- Chờ (H6 {h6_chg:+.1f}%)"
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
                verdict = f"⚠️ OUT -- H6 xanh {h6_chg:+.1f}% vol {h6_vr:.1f}x"
                urgent  = True
            else:
                verdict = f"✅ HOLD -- H6 {h6_chg:+.1f}%"
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
        urgent_msg = "🚨 <b>URGENT -- TP/SL HIT!</b>\n\n" + "\n\n".join(urgent_lines)
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
    lines = [f"⚡ <b>H2 SCAN -- {now}</b>\n"]

    def fmt_r(r: ScoreResult, section: str) -> None:
        sym    = html.escape(r.display_symbol)
        signal = html.escape(r.signal_type)
        icon   = "🟢" if "PUMP" in r.market_mode else "🔴"

        def pct(t, e):
            if e <= 0: return ""
            return f"{(t-e)/e*100:+.2f}%"

        lines.append(f"{'='*27}")
        lines.append(f"{icon} <b>{section}</b>")
        lines.append(f"{'='*27}\n")
        lines.append(
            f"<b>{sym}</b> -- <b>{r.total_score:.1f}đ</b>\n"
            f"⚡ <b>{signal}</b>\n"
            f"H2: {r.price_chg:+.2f}% | Vol: {r.vol_ratio:.1f}x | "
            f"OI: {r.oi_chg_pct:+.1f}% | FR: {r.fr:.4f}%"
        )
        if r.entry > 0 and r.tp1 > 0:
            lines.append(
                f"📍 Entry: <b>{r.entry:.6g}</b> | SL: <b>{r.sl:.6g}</b>\n"
                f"1️⃣ TP1: <b>{r.tp1:.6g}</b> ({pct(r.tp1, r.entry)}) R:R {r.rr_tp1:.1f}\n"
                f"2️⃣ TP2: <b>{r.tp2:.6g}</b> ({pct(r.tp2, r.entry)}) R:R {r.rr_tp2:.1f}\n"
                f"3️⃣ TP3: <b>{r.tp3:.6g}</b> ({pct(r.tp3, r.entry)})"
            )
        lines.append("")

    if pump:
        fmt_r(pump[0], "H2 PUMP -- MUA NGẮN HẠN")
    if dump:
        fmt_r(dump[0], "H2 DUMP -- SHORT NGẮN HẠN")
    if not pump and not dump:
        lines.append("<i>Không có H2 signal đủ điều kiện.</i>")

    lines.append("⚠️ <i>Target 15-30%. Luôn đặt SL chặt.</i>")
    return "\n".join(lines)


def job_h2_scan():
    """Chạy mỗi 2H -- scan H2 pump/dump, alert nếu có signal."""
    try:
        pump, dump = run_h2_scan()
        if not pump and not dump:
            log.info("⚡ H2 scan xong -- không có signal.")
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
        pump_results, dump_results, rev_results, h4_results, h2_results, watch_results = run_scan()

        if not pump_results and not dump_results and not h4_results and not h2_results and not watch_results:
            log.warning("Không có coin nào đủ điều kiện!")
            send_telegram("⚠️ Scan xong nhưng không tìm thấy coin nào đủ điều kiện.")
            return

        for label, lst in [("PUMP",pump_results),("DUMP",dump_results),("H4 MTF",h4_results),("H2 MTF",h2_results),("WATCH",watch_results)]:
            if lst:
                log.info(f"\n{'='*50}\n{label}\n{'='*50}")
                for i, r in enumerate(lst, 1):
                    log.info(f"{i}. {r.display_symbol}: {r.total_score:.1f}đ -- {r.signal_type}")
                    for d in r.details: log.info(f"   {d}")

        save_results(pump_results, dump_results, rev_results)
        all_signals = (pump_results or []) + (dump_results or []) + (h4_results or []) + (h2_results or [])
        register_active_trades(all_signals, source="HOURLY_SCAN")
        msg = format_alert(pump_results, dump_results, rev_results, h4_results, h2_results, watch_results)
        if send_telegram(msg):
            log.info("✅ Đã gửi Telegram alert!")
        else:
            log.error("❌ Gửi Telegram thất bại!")

    except Exception as e:
        log.error(f"Job error: {e}", exc_info=True)
        send_telegram(f"❌ Scanner error: {e}")

# ACTIVE TRADE MONITOR -- CHECK MỖI 30 PHÚT
# ==============================================================

ACTIVE_TRADES_FILE = "active_trades.json"
MONITOR_INTERVAL_MINUTES = 30
MONITOR_PRICE_ADVERSE_PCT = 1.5       # Giá đi ngược entry 1.5% thì cảnh báo thoát sớm
MONITOR_CVD_BARS = 3                  # Dùng 3 nến M30 gần nhất để làm CVD proxy
MONITOR_CVD_BEARISH_BARS = 2          # LONG: >=2/3 nến signed volume âm = xấu
MONITOR_CVD_BULLISH_BARS = 2          # SHORT: >=2/3 nến signed volume dương = xấu
MONITOR_FUNDING_LONG_MAX = 0.08       # LONG: funding > 0.08% = crowded long, xấu
MONITOR_FUNDING_SHORT_MIN = -0.08     # SHORT: funding < -0.08% = crowded short, xấu


def _active_trade_key(exchange: str, symbol: str, side: str) -> str:
    return f"{exchange}:{symbol}:{side}".upper()


def _load_active_trades() -> dict:
    try:
        with open(ACTIVE_TRADES_FILE, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save_active_trades(data: dict) -> None:
    try:
        with open(ACTIVE_TRADES_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log.warning(f"Không lưu được {ACTIVE_TRADES_FILE}: {e}")


def _result_side_for_hold(r: ScoreResult) -> str:
    side = _reversal_side(r) if getattr(r, "reversal_type", "") else ""
    if side:
        return side
    sig = (r.signal_type or r.market_mode or "").upper()
    if "SHORT" in sig or "DUMP" in sig:
        return "SHORT"
    return "LONG"


def register_active_trades(results: list[ScoreResult], source: str = "SCAN") -> None:
    """Lưu các signal mới để monitor mỗi 30 phút. Không lưu nếu thiếu entry/SL."""
    data = _load_active_trades()
    now = datetime.now(timezone.utc).isoformat()
    added = 0

    for r in results or []:
        if not r or r.entry <= 0 or r.sl <= 0:
            continue
        # Chỉ lưu pump/dump 1D và H4 MTF (bỏ reversal)
        if getattr(r, "timeframe", "1D") == "REV":
            continue
        side = _result_side_for_hold(r)
        key = _active_trade_key(r.exchange, r.symbol, side)
        data[key] = {
            "symbol": r.symbol,
            "exchange": r.exchange,
            "side": side,
            "entry": float(r.entry),
            "sl": float(r.sl),
            "tp1": float(r.tp1),
            "tp2": float(r.tp2),
            "tp3": float(r.tp3),
            "score": float(r.total_score),
            "source": source,
            "timeframe": getattr(r, "timeframe", "1D"),
            "created_at": now,
            "last_check": "",
            # Trạng thái hit -- track riêng từng mốc
            "entry_hit": False,      # Giá đã về vùng entry chưa
            "tp1_hit": False,
            "tp2_hit": False,
            "tp3_hit": False,
            "sl_hit": False,
            "exit_alerted": False,   # Dùng cho deterioration alert cũ
        }
        added += 1

    if added:
        _save_active_trades(data)
        log.info(f"📌 Đã đưa {added} signal vào active_trades để monitor.")


def _signed_cvd_proxy(candles: list, bars: int = MONITOR_CVD_BARS) -> tuple[float, int, int]:
    """Proxy CVD bằng signed volume M30: nến xanh +vol, nến đỏ -vol."""
    if not candles:
        return 0.0, 0, 0
    recent = candles[-bars:]
    cvd = 0.0
    bull = bear = 0
    for c in recent:
        o = float(c.get("o", 0)); cl = float(c.get("c", 0)); v = float(c.get("v", 0))
        if cl >= o:
            cvd += v; bull += 1
        else:
            cvd -= v; bear += 1
    return cvd, bull, bear


def _fmt_pct_value(v: float) -> str:
    return f"{v:+.2f}%"


def monitor_active_trades() -> None:
    """
    Monitor mỗi 30 phút -- chỉ cho coin từ TOP PUMP / TOP DUMP 1D + H4 MTF.

    Alert khi:
    ---------------------------------------------------------
    ✅ ENTRY HIT  -- giá chạm vùng entry (±0.5%) lần đầu
    🎯 TP1/TP2/TP3 HIT -- giá chạm từng mốc chốt lời
    🛑 SL HIT     -- giá chạm stop loss
    ---------------------------------------------------------
    Mỗi mốc chỉ alert 1 lần (không spam).
    Sau khi SL hoặc TP3 hit -> đánh dấu exit_alerted để dừng monitor.
    """
    data = _load_active_trades()
    if not data:
        log.info("👁️ Monitor: không có coin đang theo dõi.")
        return

    now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    alert_blocks: list[str] = []
    changed = False
    ENTRY_TOLERANCE = 0.005   # ±0.5% vùng entry

    for key, t in list(data.items()):
        # Bỏ qua nếu đã exit hoàn toàn (SL hoặc TP3)
        if t.get("exit_alerted") or t.get("sl_hit"):
            continue

        symbol   = t.get("symbol", "")
        exchange = t.get("exchange", "")
        side     = t.get("side", "LONG")
        entry    = float(t.get("entry", 0) or 0)
        sl       = float(t.get("sl", 0) or 0)
        tp1      = float(t.get("tp1", 0) or 0)
        tp2      = float(t.get("tp2", 0) or 0)
        tp3      = float(t.get("tp3", 0) or 0)
        score    = float(t.get("score", 0) or 0)
        tf       = t.get("timeframe", "1D")

        if not symbol or not exchange or entry <= 0:
            continue

        try:
            # Lấy giá hiện tại qua nến H1 (đủ nhanh, không cần tick)
            candles = get_ohlcv_1h(exchange, symbol, limit=3) or []
            if not candles:
                continue
            last = candles[-1]
            price = float(last.get("c", 0))
            if price <= 0:
                continue

            t["last_check"] = datetime.now(timezone.utc).isoformat()
            t["last_price"]  = price
            pnl = (price - entry) / entry * 100 if side == "LONG" else (entry - price) / entry * 100
            t["last_pnl_pct"] = round(pnl, 2)
            changed = True

            sym_esc = html.escape(f"{symbol} . {exchange}")
            side_icon = "🟢 LONG" if side == "LONG" else "🔻 SHORT"
            tf_tag    = f"[{tf}]" if tf != "1D" else ""

            def _fmt(v): return f"{v:.6g}" if v > 0 else "-"

            # -- SL HIT ------------------------------------------
            sl_hit = (sl > 0) and (
                (side == "LONG"  and price <= sl) or
                (side == "SHORT" and price >= sl)
            )
            if sl_hit and not t.get("sl_hit"):
                t["sl_hit"]       = True
                t["exit_alerted"] = True
                changed = True
                alert_blocks.append(
                    f"🛑 <b>SL HIT</b> {tf_tag}\n"
                    f"<b>{sym_esc}</b> | {side_icon} | {score:.1f}đ\n"
                    f"Entry: <b>{_fmt(entry)}</b> -> Giá: <b>{_fmt(price)}</b>\n"
                    f"SL: <b>{_fmt(sl)}</b> | PnL: <b>{pnl:+.2f}%</b> ❌"
                )
                log.info(f"🛑 SL hit: {symbol} . {exchange}")
                continue  # Không check TP sau khi SL

            # -- ENTRY HIT ----------------------------------------
            if not t.get("entry_hit"):
                near_entry = abs(price - entry) / entry <= ENTRY_TOLERANCE
                entry_crossed = (
                    (side == "LONG"  and price <= entry * (1 + ENTRY_TOLERANCE)) or
                    (side == "SHORT" and price >= entry * (1 - ENTRY_TOLERANCE))
                )
                if near_entry or entry_crossed:
                    t["entry_hit"] = True
                    changed = True
                    sl_dist_pct = abs(entry - sl) / entry * 100 if sl > 0 else 0
                    alert_blocks.append(
                        f"✅ <b>ENTRY HIT</b> {tf_tag}\n"
                        f"<b>{sym_esc}</b> | {side_icon} | {score:.1f}đ\n"
                        f"⚡ Giá: <b>{_fmt(price)}</b> -- Entry: <b>{_fmt(entry)}</b>\n"
                        f"🛑 SL: <b>{_fmt(sl)}</b> (cách {sl_dist_pct:.1f}%)\n"
                        f"1️⃣ TP1: <b>{_fmt(tp1)}</b>  2️⃣ TP2: <b>{_fmt(tp2)}</b>  3️⃣ TP3: <b>{_fmt(tp3)}</b>"
                    )
                    log.info(f"✅ Entry hit: {symbol} . {exchange} @ {price:.6g}")

            # -- TP HIT (chỉ check sau khi đã entry) -------------
            if not t.get("entry_hit"):
                continue  # Chưa vào lệnh, bỏ qua TP check

            tp_checks = [
                ("tp3_hit", tp3, "3️⃣ TP3 🎯🎯🎯", True),   # exit=True
                ("tp2_hit", tp2, "2️⃣ TP2 🎯🎯",  False),
                ("tp1_hit", tp1, "1️⃣ TP1 🎯",    False),
            ]
            for field_name, tp_price, label, is_final in tp_checks:
                if tp_price <= 0 or t.get(field_name):
                    continue
                tp_hit = (
                    (side == "LONG"  and price >= tp_price) or
                    (side == "SHORT" and price <= tp_price)
                )
                if tp_hit:
                    t[field_name] = True
                    if is_final:
                        t["exit_alerted"] = True
                    changed = True
                    profit_pct = abs(tp_price - entry) / entry * 100
                    note = " -- <i>Cân nhắc chốt toàn bộ</i>" if is_final else " -- <i>Chốt 1 phần, dời SL lên entry</i>"
                    alert_blocks.append(
                        f"{label} <b>HIT</b> {tf_tag}\n"
                        f"<b>{sym_esc}</b> | {side_icon} | {score:.1f}đ\n"
                        f"Giá: <b>{_fmt(price)}</b> | TP: <b>{_fmt(tp_price)}</b> (+{profit_pct:.1f}%)\n"
                        f"Entry: <b>{_fmt(entry)}</b> | SL: <b>{_fmt(sl)}</b>{note}"
                    )
                    log.info(f"🎯 {label} hit: {symbol} . {exchange} @ {price:.6g}")

        except Exception as e:
            log.debug(f"Monitor lỗi {key}: {e}")

    if changed:
        _save_active_trades(data)

    if alert_blocks:
        msg = f"👁️ <b>PRICE MONITOR -- {now_str}</b>\n\n" + "\n\n".join(alert_blocks)
        if send_telegram(msg):
            log.info(f"📢 Monitor gửi {len(alert_blocks)} alert")
        else:
            log.error("❌ Monitor gửi alert thất bại")
    else:
        log.info("👁️ Monitor: chưa có mốc nào bị chạm.")

# ==============================================================
# ENTRY POINT
# ==============================================================

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

        log.info(f"🧪 Test coin: {symbol} . {exchange}")
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
                print(f"⚠️ {symbol} . {exchange}: Điểm thấp hơn ngưỡng {MIN_SCORE}")
        else:
            print(f"❌ Không lấy được data cho {symbol} . {exchange}")

    else:
        # -- SCHEDULER V7.2 FIXED ---------------------------------
        # xx:02 UTC -> Full scan mỗi giờ: PUMP + DUMP + REVERSAL
        # xx:17 / xx:47 UTC -> Monitor coin đang hold: Price + CVD proxy + Funding
        # Fix: KHÔNG dùng next_hourly_slot_utc() trong loop vì dễ miss xx:02 nếu bot thức dậy sau vài giây.
        # Logic mới check theo phút hiện tại, chạy đúng 1 lần mỗi slot.

        FULL_SCAN_MINUTE = 2
        MONITOR_MINUTES = (17, 47)
        CHECK_SLEEP = 10

        def slot_id(dt):
            return dt.strftime("%Y%m%d%H%M")

        log.info("⏰ SCHEDULER V7.2 FIXED khởi động")
        log.info("   xx:02 UTC -> Full scan mỗi giờ: PUMP + DUMP + REVERSAL")
        log.info("   xx:17 / xx:47 UTC -> Monitor coin đang hold, alert THOÁT nếu deteriorate")
        log.info(f"   Sàn quét: {' | '.join(SCAN_EXCHANGES)}")

        last_full_slot = ""
        last_monitor_slot = ""

        while True:
            now = datetime.now(timezone.utc)
            current_slot = slot_id(now)

            # Full scan hourly -- chạy 1 lần trong phút xx:02 UTC
            if now.minute == FULL_SCAN_MINUTE and current_slot != last_full_slot:
                last_full_slot = current_slot
                scan_start_dt = datetime.now(timezone.utc)
                scan_start = time.time()
                try:
                    log.info(f"🚀 [{scan_start_dt.strftime('%H:%M:%S UTC')}] Full scan hourly...")
                    job()
                except Exception as e:
                    log.error(f"Scheduler hourly error: {e}", exc_info=True)
                    try:
                        send_telegram(f"❌ [hourly] error: {html.escape(str(e))}")
                    except Exception:
                        pass
                elapsed = time.time() - scan_start
                log.info(f"✅ Full scan xong {elapsed:.0f}s")

            # Monitor active trades -- chạy 1 lần trong phút xx:17 và xx:47 UTC
            if now.minute in MONITOR_MINUTES and current_slot != last_monitor_slot:
                last_monitor_slot = current_slot
                try:
                    log.info(f"👁️ [{now.strftime('%H:%M:%S UTC')}] Monitor active trades...")
                    monitor_active_trades()
                except Exception as e:
                    log.error(f"Monitor scheduler error: {e}", exc_info=True)
                    try:
                        send_telegram(f"❌ [monitor] error: {html.escape(str(e))}")
                    except Exception:
                        pass

            # Log nhẹ để biết bot còn sống, không spam mỗi 10s quá nhiều
            if now.second < CHECK_SLEEP:
                next_full_h = now.hour if now.minute < FULL_SCAN_MINUTE else (now.hour + 1) % 24
                log.info(
                    f"⏳ Alive {now.strftime('%H:%M:%S UTC')} | Full: {next_full_h:02d}:{FULL_SCAN_MINUTE:02d} UTC | "
                    f"Monitor: xx:17 / xx:47"
                )

            time.sleep(CHECK_SLEEP)