import requests
import psycopg2
from datetime import datetime, timedelta
from airflow.decorators import dag, task

# DB connection info for Neon
DB_SETTINGS = {
    "host": "ep-bold-darkness-a2urmndi-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_JZVW2N1aAObX",
    "port": "5432",
    "sslmode": "require",
    "options": "-c channel_binding=require"
}

SYMBOL = "BTCUSDT"
INTERVAL = "5m"
LIMIT = 100

# Indicator functions (same as your script)
def sma(values, window):
    if len(values) < window:
        return None
    return sum(values[-window:]) / window

def ema(values, window, prev_ema=None):
    if len(values) < window:
        return None
    k = 2 / (window + 1)
    if prev_ema is None:
        return sma(values, window)
    return values[-1] * k + prev_ema * (1 - k)

def rsi(values, window=14):
    if len(values) < window + 1:
        return None
    gains = 0
    losses = 0
    for i in range(-window, 0):
        diff = values[i] - values[i - 1]
        if diff > 0:
            gains += diff
        else:
            losses -= diff
    if losses == 0:
        return 100
    rs = gains / losses
    return 100 - (100 / (1 + rs))


@dag(
    schedule_interval="*/5 * * * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["binance", "neon", "indicators"]
)
def binance_indicator_etl():

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def fetch_binance_data():
        url = f"https://api.binance.com/api/v3/klines?symbol={SYMBOL}&interval={INTERVAL}&limit={LIMIT}"
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    @task()
    def process_and_save(data):
        conn = psycopg2.connect(**DB_SETTINGS)
        cursor = conn.cursor()

        insert_query = """
        INSERT INTO binance_ohlcv_indicators (
            open_time, open, high, low, close, volume, sma, ema, rsi
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (open_time) DO NOTHING;
        """

        closes = []
        ema_prev = None

        for row in data:
            open_time = datetime.fromtimestamp(row[0] / 1000.0)
            open_, high, low, close, volume = map(float, row[1:6])
            closes.append(close)

            sma_val = sma(closes, 14)
            ema_val = ema(closes, 14, ema_prev)
            ema_prev = ema_val if ema_val is not None else ema_prev
            rsi_val = rsi(closes, 14)

            cursor.execute(insert_query, (
                open_time, open_, high, low, close, volume,
                sma_val, ema_val, rsi_val
            ))

        conn.commit()
        cursor.close()
        conn.close()

    raw_data = fetch_binance_data()
    process_and_save(raw_data)

dag = binance_indicator_etl()
