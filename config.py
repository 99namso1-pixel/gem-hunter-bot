# ============================================================
# CONFIG — PUMP SCANNER BOT
# Điền thông tin của bạn vào đây
# ============================================================

# ── API Keys ─────────────────────────────────────────────────
COINGLASS_API_KEY  = "a018f77485404625b415d3351dccc70e"
TELEGRAM_TOKEN     = "8702072641:AAHquqm7NZGlOHyOrEdJ4-skLrVykFGESDc"
TELEGRAM_CHAT_ID   = "-1003831313490"

# ── Scanner Settings ─────────────────────────────────────────
TOP_N              = 5       # Số coin top hiển thị trong alert
MIN_SCORE          = 4.0     # Điểm tối thiểu để lọc coin (0-10.5)
SCAN_INTERVAL_HOURS = 24     # Scan mỗi bao nhiêu giờ (24 = 1 lần/ngày)

# ── Coins bị loại trừ (stablecoin, BTC, ETH, top coins) ─────
EXCLUDE_SYMBOLS = {
    "BTCUSDT", "ETHUSDT", "BNBUSDT", "SOLUSDT", "XRPUSDT",
    "ADAUSDT", "DOGEUSDT", "AVAXUSDT", "DOTUSDT", "MATICUSDT",
    "LINKUSDT", "UNIUSDT", "LTCUSDT", "ATOMUSDT", "ETCUSDT",
    "FILUSDT", "AAVEUSDT", "MKRUSDT", "COMPUSDT", "SNXUSDT",
    # Stablecoins
    "USDCUSDT", "BUSDUSDT", "TUSDUSDT", "USDPUSDT",
    # Wrapped tokens
    "WBTCUSDT", "WETHUSDT",
}

# ── Scoring Thresholds ───────────────────────────────────────

# Volume
VOL_SPIKE_MIN      = 2.0     # Vol / MA10 tối thiểu để tính CVB

# Open Interest
OI_DIV_MIN_PCT     = 5.0     # OI tăng tối thiểu % trong 4 nến

# Funding Rate
FR_MAX_NORMAL      = 0.02    # FR tối đa bình thường (%) — dương thấp
FR_SQUEEZE_THRESHOLD = -0.5  # FR âm để xét Short Squeeze (%)

# Long/Short Ratio
LSR_MIN            = 0.95    # L/S Ratio tối thiểu (buyers >= sellers)

# Liquidation
LIQ_RATIO_MIN_GOOD = 1.5     # Shorts liq / Longs liq tối thiểu = tốt

# Price near bottom
BOTTOM_PCT         = 30.0    # Giá trong X% so với đáy 20 nến = "ở đáy"

# ── Score Weights (tham khảo) ─────────────────────────────────
# CVB mạnh:        3.0đ
# CVB thường:      2.0đ
# Vol Spike:       1.0đ
# OI Div mạnh:     2.0đ
# OI Div thường:   1.0đ
# FR tốt:          2.0đ
# FR ổn:           1.0đ
# L/S tốt:         1.0đ
# Liq tốt (2x+):   2.0đ
# Liq ổn:          1.0đ
# Squeeze bonus:   1.0đ
# Nến đẹp:         0.5đ
# MAX TOTAL:       10.5đ
