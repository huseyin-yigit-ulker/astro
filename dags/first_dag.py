import logging
from datetime import datetime, timedelta
from typing import List, Dict, Any

import requests
import psycopg2
from airflow.sdk import dag, task
from airflow.timetables.cron import CronTriggerTimetable  # <-- import CronTriggerTimetable


DB_SETTINGS = {
    "host": "ep-bold-darkness-a2urmndi-pooler.eu-central-1.aws.neon.tech",
    "dbname": "neondb",
    "user": "neondb_owner",
    "password": "npg_JZVW2N1aAObX",
    "port": "5432"
}

SYMBOL = "BTCUSDT"
INTERVAL = "5m"
LIMIT = 50


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


@dag(schedule_interval="*/5 * * * *",  catchup=False)
def binance_5m_etl():

    @task(task_id="extract_binance_data", retries=2)
    def extract_binance_data() -> List[List]:
        url = f"https://api.binance.com/api/v3/klines?symbol={SYMBOL}&interval={INTERVAL}&limit={LIMIT}"
        logging.info(f"Fetching data from {url}")
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    @task(task_id="process_last_row")
    def process_last_row(raw_data: List[List]) -> Dict[str, Any]:
        closes = []
        ema_prev = None

        for row in raw_data:
            close = float(row[4])
            closes.append(close)

            sma_val = sma(closes, 14)
            ema_val = ema(closes, 14, ema_prev)
            ema_prev = ema_val if ema_val is not None else ema_prev
            rsi_val = rsi(closes, 14)

            open_time = datetime.fromtimestamp(row[0] / 1000.0)
            open_, high, low, close_, volume = map(float, row[1:6])

            if row == raw_data[-1]:
                return dict(
                    open_time=open_time,
                    open=open_,
                    high=high,
                    low=low,
                    close=close_,
                    volume=volume,
                    sma=sma_val,
                    ema=ema_val,
                    rsi=rsi_val,
                )

    @task(task_id="save_to_postgres")
    def save_to_postgres(row: Dict[str, Any]):
        logging.info(f"Inserting last indicator row to PostgreSQL")
        conn = psycopg2.connect(**DB_SETTINGS)
        cursor = conn.cursor()

        insert_query = """
            INSERT INTO binance_ohlcv_indicators
            (open_time, open, high, low, close, volume, sma, ema, rsi)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (open_time) DO NOTHING;
        """

        cursor.execute(insert_query, (
            row["open_time"],
            row["open"],
            row["high"],
            row["low"],
            row["close"],
            row["volume"],
            row["sma"],
            row["ema"],
            row["rsi"],
        ))

        conn.commit()
        cursor.close()
        conn.close()
        logging.info(f"Inserted final row into PostgreSQL")

    raw = extract_binance_data()
    last_row = process_last_row(raw)
    save_to_postgres(last_row)


binance_5m_etl()
