import logging
from datetime import datetime, timedelta
from typing import List, Dict

import requests
import psycopg2
from airflow.sdk import dag, task

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


def sma(values: List[float], window: int):
    if len(values) < window:
        return None
    return sum(values[-window:]) / window


def ema(values: List[float], window: int, prev_ema=None):
    if len(values) < window:
        return None
    k = 2 / (window + 1)
    if prev_ema is None:
        return sma(values, window)
    return values[-1] * k + prev_ema * (1 - k)


def rsi(values: List[float], window: int = 14):
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


@dag()
def binance_5m_etl():

    @task(task_id="extract_binance_data", retries=2)
    def extract_binance_data() -> List[List]:
        url = f"https://api.binance.com/api/v3/klines?symbol={SYMBOL}&interval={INTERVAL}&limit={LIMIT}"
        logging.info(f"Fetching data from {url}")
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    @task(multiple_outputs=True)
    def process_and_calc_indicators(raw_data: List[List]) -> Dict[str, List]:
        closes = []
        open_times = []
        opens = []
        highs = []
        lows = []
        volumes = []

        sma_list = []
        ema_list = []
        rsi_list = []

        ema_prev = None

        for row in raw_data:
            open_time = datetime.fromtimestamp(row[0] / 1000.0)
            open_, high, low, close, volume = map(float, row[1:6])

            open_times.append(open_time)
            opens.append(open_)
            highs.append(high)
            lows.append(low)
            closes.append(close)
            volumes.append(volume)

            sma_val = sma(closes, 14)
            ema_val = ema(closes, 14, ema_prev)
            ema_prev = ema_val if ema_val is not None else ema_prev
            rsi_val = rsi(closes, 14)

            sma_list.append(sma_val)
            ema_list.append(ema_val)
            rsi_list.append(rsi_val)

        logging.info(f"Processed {len(raw_data)} rows with indicators")

        return dict(
            open_time=open_times,
            open=opens,
            high=highs,
            low=lows,
            close=closes,
            volume=volumes,
            sma=sma_list,
            ema=ema_list,
            rsi=rsi_list,
        )

    @task(task_id="save_to_postgres")
    def save_to_postgres(data: Dict[str, List]):
        logging.info(f"started to postgre")
        conn = psycopg2.connect(**DB_SETTINGS)
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO binance_ohlcv_indicators
            (open_time, open, high, low, close, volume, sma, ema, rsi)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (open_time) DO NOTHING;
        """

        rows_to_insert = list(
            zip(
                data["open_time"],
                data["open"],
                data["high"],
                data["low"],
                data["close"],
                data["volume"],
                data["sma"],
                data["ema"],
                data["rsi"],
            )
        )

        cursor.executemany(insert_query, rows_to_insert)
        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Inserted {len(rows_to_insert)} rows into PostgreSQL")

    raw = extract_binance_data()
    processed = process_and_calc_indicators(raw)
    save_to_postgres(processed)


binance_5m_etl()
