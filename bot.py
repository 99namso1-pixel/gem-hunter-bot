#!/usr/bin/env python3
"""
╔══════════════════════════════════════════════════════════════╗
║  CRYPTO PUMP SCANNER BOT V4 — TREND + SQUEEZE ENGINE                  ║
║  Quét toàn bộ USDT Perp Binance & Bybit                     ║
║  Auto scan 1H + Trend/Squeeze engine → Telegram      ║
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
SCAN_EXCHANGES = ["Binance", "Bybit", "BingX"]  # Thêm BingX
PER_EXCHANGE_TOP_N = False             # True = lấy top mỗi sàn, False = gộp chung top
TOP_N_FINAL = 3                         # Chỉ gửi 3 coin tiềm năng nhất
AUTO_SCAN_INTERVAL_SECONDS = 3600       # Scan tự động mỗi 1 giờ
MIN_VOL_RATIO_FILTER = 2.0              # Tăng 1.2→2.0: loại noise MOG/1INCH vol thấp
MIN_PRICE_CHANGE_FILTER = 5.0           # Loại coin tăng quá yếu nếu volume không đủ
MAX_LSR_HEALTHY = 2.30                  # L/S quá cao = crowded long, giảm điểm

# Engine mode
TREND_MIN_SCORE = 5.0                   # Ngưỡng nhận diện TREND coin kiểu IRYS
SQUEEZE_MIN_SCORE = 5.0                 # Ngưỡng nhận diện SQUEEZE coin kiểu COS
HYBRID_MIN_SCORE = 5.0                  # Cả trend + squeeze đều mạnh

# Tăng tốc scan
FAST_SCAN = True
MAX_WORKERS_BINANCE = 12   # Nếu bị rate-limit thì giảm còn 6-8
MAX_WORKERS_BYBIT  = 10    # Nếu bị rate-limit thì giảm còn 5-8
MAX_WORKERS_BINGX  = 8     # BingX rate limit thấp hơn
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
    details: list = field(default_factory=list)

    @property
    def display_symbol(self) -> str:
        return f"{self.symbol} · {self.exchange}"


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
    raise ValueError(f"Unsupported exchange: {exchange}")


# ══════════════════════════════════════════════════════════════
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
    return None


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

    # Bỏ nến đỏ để ưu tiên pump/reversal đang bật
    if coin.close < coin.open:
        return None

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
    # Không sleep ở đây: bản FAST dùng ThreadPool + timeout ngắn

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

def score_coin(coin: CoinData) -> Optional[ScoreResult]:
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

    # 0. Momentum thật: ưu tiên coin đang chạy mạnh như IRYS
    if coin.price_change_pct >= 20 and vol_ratio >= 2:
        result.score_momentum = 3.0
        details.append(f"🚀 Momentum mạnh (+{coin.price_change_pct:.1f}%)")
    elif coin.price_change_pct >= 12 and vol_ratio >= 1.5:
        result.score_momentum = 2.0
        details.append(f"🚀 Momentum (+{coin.price_change_pct:.1f}%)")
    elif coin.price_change_pct >= 7:
        result.score_momentum = 1.0
        details.append(f"📈 Giá tăng (+{coin.price_change_pct:.1f}%)")

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

    # 2. OI Divergence
    abs_price_chg = abs(coin.price_change_pct)
    if coin.oi_change_pct >= 50 and coin.oi_change_pct > abs_price_chg:
        result.score_oi_div = 3.0
        details.append(f"📡 OI Div cực mạnh (+{coin.oi_change_pct:.1f}%)")
    elif coin.oi_change_pct >= OI_DIV_MIN_PCT and coin.oi_change_pct > abs_price_chg * 1.5:
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

    # 5. Liquidation direction
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
    return result


# ══════════════════════════════════════════════════════════════
# SCANNER
# ══════════════════════════════════════════════════════════════

def scan_one_symbol(exchange: str, symbol: str) -> Optional[ScoreResult]:
    """Scan 1 coin. Tách riêng để chạy đa luồng."""
    coin = fetch_coin_data(exchange, symbol)
    if coin is None:
        return None
    return score_coin(coin)


def run_scan_exchange(exchange: str) -> list[ScoreResult]:
    log.info("=" * 60)
    log.info(f"🔍 FAST SCAN {exchange} — {datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M UTC')}")
    log.info("=" * 60)

    symbols = get_all_symbols(exchange)
    if not symbols:
        log.error(f"Không lấy được danh sách symbols {exchange}!")
        return []

    results: list[ScoreResult] = []
    errors = 0
    workers = MAX_WORKERS_BINANCE if exchange == "Binance" else MAX_WORKERS_BINGX if exchange == "BingX" else MAX_WORKERS_BYBIT

    log.info(f"🚀 {exchange}: scanning {len(symbols)} symbols với {workers} workers...")

    if not FAST_SCAN:
        for i, symbol in enumerate(symbols, 1):
            try:
                if i == 1 or i % LOG_EVERY_N == 0 or i == len(symbols):
                    log.info(f"[{exchange} {i}/{len(symbols)}] Scanning...")
                score = scan_one_symbol(exchange, symbol)
                if score:
                    results.append(score)
                    log.info(f"  ✅ {score.display_symbol}: {score.total_score:.1f}đ — {score.signal_type}")
            except Exception as e:
                errors += 1
                log.warning(f"  ❌ {exchange} {symbol}: {e}")
        results.sort(key=lambda x: x.total_score, reverse=True)
        log.info(f"✅ {exchange} scan xong | found {len(results)} | errors {errors}")
        return results

    completed = 0
    with ThreadPoolExecutor(max_workers=workers) as executor:
        future_map = {executor.submit(scan_one_symbol, exchange, s): s for s in symbols}

        for future in as_completed(future_map):
            symbol = future_map[future]
            completed += 1
            try:
                score = future.result()
                if score:
                    results.append(score)
                    log.info(f"  ✅ {score.display_symbol}: {score.total_score:.1f}đ — {score.signal_type}")
            except Exception as e:
                errors += 1
                log.debug(f"  ❌ {exchange} {symbol}: {e}")

            if completed == 1 or completed % LOG_EVERY_N == 0 or completed == len(symbols):
                log.info(f"[{exchange}] Progress {completed}/{len(symbols)} | found {len(results)} | errors {errors}")

    results.sort(key=lambda x: x.total_score, reverse=True)
    log.info(f"✅ {exchange} FAST scan xong: {len(symbols)} symbols | found {len(results)} | errors {errors}")
    return results

def run_scan() -> list[ScoreResult]:
    all_results: list[ScoreResult] = []

    for exchange in SCAN_EXCHANGES:
        ex_results = run_scan_exchange(exchange)
        if PER_EXCHANGE_TOP_N:
            all_results.extend(ex_results[:TOP_N_FINAL])
        else:
            all_results.extend(ex_results)

    # Dedup: giữ score cao nhất mỗi coin (tránh IRYS Binance + IRYS Bybit trùng nhau)
    seen_coins: dict[str, ScoreResult] = {}
    for r in sorted(all_results, key=lambda x: x.total_score, reverse=True):
        base = r.symbol.upper()
        if base not in seen_coins:
            seen_coins[base] = r
    unique = list(seen_coins.values())

    # Tách SQUEEZE và TREND/MOMENTUM
    squeezes = [r for r in unique if r.market_mode == "SQUEEZE" or r.squeeze_engine_score >= SQUEEZE_MIN_SCORE]
    others   = [r for r in unique if r not in squeezes]

    # Sắp xếp từng nhóm theo điểm
    squeezes.sort(key=lambda x: x.squeeze_engine_score, reverse=True)
    others.sort(key=lambda x: x.total_score, reverse=True)

    # Ưu tiên: SQUEEZE lên #1 nếu có, rồi ghép với phần còn lại
    if squeezes:
        final = [squeezes[0]] + others[:TOP_N_FINAL - 1]
    else:
        final = others[:TOP_N_FINAL]

    return final[:TOP_N_FINAL]


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


def format_alert(results: list[ScoreResult]) -> str:
    now = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    lines = [
        f"🚀 <b>PUMP SCANNER V4 — TREND + SQUEEZE</b>",
        f"🕒 <b>{now}</b>",
        f"📊 <b>TOP {len(results)} COIN TIỀM NĂNG NHẤT</b>\n",
    ]

    # Telegram không hỗ trợ màu chữ thật. Dùng badge màu + bold để phân biệt Top 1/2/3.
    rank_styles = [
        ("🟢🥇", "TOP 1 — ƯU TIÊN MẠNH"),
        ("🟡🥈", "TOP 2 — THEO DÕI MẠNH"),
        ("🟠🥉", "TOP 3 — WATCHLIST"),
    ]

    for i, r in enumerate(results[:3]):
        badge, rank_name = rank_styles[i] if i < len(rank_styles) else ("⭐", "WATCHLIST")
        # Đổi badge theo mode
        if r.market_mode == "SQUEEZE":
            mode_icon = "🔴"
        elif r.market_mode == "TREND":
            mode_icon = "🟢"
        elif r.market_mode == "HYBRID":
            mode_icon = "🟣"
        else:
            mode_icon = "⚡"

        symbol   = html.escape(str(r.symbol))
        exchange = html.escape(str(r.exchange))
        signal   = html.escape(str(r.signal_type))
        details  = html.escape(" | ".join(str(x) for x in r.details[:4]))
        engine_info = ""
        if r.market_mode in ("SQUEEZE", "HYBRID"):
            engine_info = f" | 🔴SQ:{r.squeeze_engine_score:.1f}"
        elif r.market_mode == "TREND":
            engine_info = f" | 🟢TR:{r.trend_score:.1f}"

        lines.append(
            f"{badge} <b>{rank_name}</b>\n"
            f"<b>{symbol}</b> — {exchange} — <b>{r.total_score:.1f}đ</b>{engine_info}\n"
            f"{mode_icon} <b>{signal}</b>\n"
            f"💰 Giá: <b>{r.price_current:.6g}</b> | Low ngày: {r.day_low:.6g} | +{r.price_chg:.2f}%\n"
            f"Vol: {r.vol_ratio}x | OI: {r.oi_chg_pct:+.1f}% | "
            f"FR: {r.fr:.4f}% | L/S: {r.lsr:.3f}\n"
            f"Liq: {r.liq_ratio:.1f}x"
        )
        if details:
            lines.append(f"<i>{details}</i>")
        lines.append("")

    lines.append("⚠️ <i>Không phải lời khuyên đầu tư. Luôn đặt SL.</i>")
    return "\n".join(lines)


def save_results(results: list[ScoreResult]) -> str:
    import os
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M")
    filename = f"results/scan_multi_{timestamp}.json"
    os.makedirs("results", exist_ok=True)

    data = []
    for r in results:
        data.append({
            "exchange": r.exchange,
            "symbol": r.symbol,
            "score": r.total_score,
            "signal_type": r.signal_type,
            "vol_ratio": r.vol_ratio,
            "oi_chg_pct": r.oi_chg_pct,
            "fr": r.fr,
            "lsr": r.lsr,
            "liq_ratio": r.liq_ratio,
            "price_chg": r.price_chg,
            "price_current": r.price_current,
            "day_low": r.day_low,
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
        })

    with open(filename, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    log.info(f"💾 Kết quả lưu: {filename}")
    return filename


# ══════════════════════════════════════════════════════════════
# MAIN JOB
# ══════════════════════════════════════════════════════════════

def job():
    try:
        results = run_scan()

        if not results:
            log.warning("Không có coin nào đủ điều kiện!")
            send_telegram("⚠️ Scan xong nhưng không tìm thấy coin nào đủ điều kiện.")
            return

        log.info("\n" + "=" * 60)
        log.info("🏆 KẾT QUẢ TOP COINS")
        log.info("=" * 60)
        for i, r in enumerate(results, 1):
            log.info(f"{i}. {r.display_symbol}: {r.total_score:.1f}đ — {r.market_mode} — {r.signal_type}")
            for d in r.details:
                log.info(f"   {d}")

        save_results(results)
        msg = format_alert(results)
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
        # ── AUTO SCAN EVERY 1 HOUR ─────────────────────────────
        # Logic mới:
        #   1. Scan ngay khi bật bot
        #   2. Sau đó canh đúng mỗi 3600 giây tính từ lúc BẮT ĐẦU scan trước
        #   3. Nếu scan quá lâu > 1 giờ thì scan tiếp sau 60 giây, không bị kẹt/miss khung
        #   4. Gửi Telegram mỗi vòng, kể cả khi không có coin đủ điều kiện

        import os
        from datetime import timedelta

        RUN_IMMEDIATELY_ON_START = True
        FIXED_SCAN_INTERVAL_SECONDS = int(os.getenv("SCAN_INTERVAL_SECONDS", AUTO_SCAN_INTERVAL_SECONDS))
        MIN_SLEEP_AFTER_LONG_SCAN = 60

        log.info("⏰ AUTO SCAN khởi động — gửi Telegram mỗi 1 giờ")
        log.info("   Chạy '--now' để test 1 lần rồi thoát")
        log.info("   Test 1 coin: python bot.py --test-one SOLUSDT Binance")
        log.info("   Có thể đổi chu kỳ bằng env SCAN_INTERVAL_SECONDS=3600")

        next_scan_at = time.time() if RUN_IMMEDIATELY_ON_START else time.time() + FIXED_SCAN_INTERVAL_SECONDS

        while True:
            now_ts = time.time()

            if now_ts < next_scan_at:
                wait = next_scan_at - now_ts
                next_dt = datetime.fromtimestamp(next_scan_at, tz=timezone.utc)
                log.info(
                    f"⏳ Đợi {wait/60:.1f} phút đến "
                    f"{next_dt.strftime('%Y-%m-%d %H:%M UTC')} để scan..."
                )
                time.sleep(min(wait, 60))
                continue

            scan_start_ts = time.time()
            hhmm = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            log.info(f"🚀 [{hhmm}] Bắt đầu scan...")

            try:
                job()
            except Exception as e:
                log.error(f"Main loop error: {e}", exc_info=True)
                try:
                    send_telegram(f"❌ Scanner main loop error: {html.escape(str(e))}")
                except Exception:
                    pass

            elapsed = time.time() - scan_start_ts

            # Lần kế tiếp = đúng 1 giờ sau thời điểm BẮT ĐẦU scan vòng hiện tại.
            # Nếu scan quá lâu và đã quá giờ kế tiếp, tránh chạy dồn liên tục bằng cách nghỉ tối thiểu 60s.
            planned_next = scan_start_ts + FIXED_SCAN_INTERVAL_SECONDS
            if time.time() >= planned_next:
                next_scan_at = time.time() + MIN_SLEEP_AFTER_LONG_SCAN
                log.warning(
                    f"⚠️ Scan mất {elapsed/60:.1f} phút, dài hơn chu kỳ. "
                    f"Sẽ scan tiếp sau {MIN_SLEEP_AFTER_LONG_SCAN}s."
                )
            else:
                next_scan_at = planned_next

            next_dt = datetime.fromtimestamp(next_scan_at, tz=timezone.utc)
            log.info(
                f"✅ Scan xong trong {elapsed:.0f}s — lần tiếp theo: "
                f"{next_dt.strftime('%Y-%m-%d %H:%M UTC')}"
            )
