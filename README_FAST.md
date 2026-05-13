# PUMP SCANNER FAST

Bản tăng tốc dùng ThreadPoolExecutor để scan Binance + Bybit song song.

## Cách dùng

Copy `bot.py` đè vào folder bot hiện tại, giữ nguyên `config.py` cũ.

```bash
python bot.py --now
```

Test 1 coin:

```bash
python bot.py --test-one SOLUSDT Binance
python bot.py --test-one SOLAYERUSDT Bybit
```

## Chỉnh tốc độ

Trong đầu file `bot.py`:

```python
MAX_WORKERS_BINANCE = 12
MAX_WORKERS_BYBIT = 10
LOG_EVERY_N = 25
```

Nếu bị rate-limit hoặc request lỗi nhiều, giảm workers còn 6-8.
