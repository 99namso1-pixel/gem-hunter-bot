# PUMP SCANNER STABLE — Binance + Bybit

Bản này đã sửa để tránh lỗi Coinglass 500 làm bot bị kẹt.

## Điểm mới
- Binance/Bybit public API lấy OHLCV.
- Binance/Bybit public API lấy Funding.
- Binance/Bybit public API lấy Open Interest history.
- Binance public API lấy Long/Short Ratio.
- Coinglass Liquidation mặc định tắt để scan nhanh và ổn định.

## Chạy
```bash
pip install -r requirements.txt
python bot.py --now
```

## Test 1 coin
```bash
python bot.py --test-one SOLUSDT Binance
python bot.py --test-one SOLAYERUSDT Bybit
```

## Muốn bật lại Coinglass liquidation
Mở bot.py và đổi:
```python
USE_COINGLASS_LIQ = True
```
