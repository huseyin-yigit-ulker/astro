import logging
from datetime import datetime, timedelta
from typing import Dict, List

import pandas as pd
import requests
import ta
from airflow.decorators import dag, task
from airflow.providers.postgres.hooks.postgres import PostgresHook

SYMBOL = "BTCUSDT"
INTERVAL = "5m"
LIMIT = 100
POSTGRES_CONN_ID = "postgres_binance"

@dag(
    schedule_interval="*/5 * * * *",  # Every 5 minutes
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["binance", "indicators", "taskflow"]
)
def binance_5m_indicator_etl():

    @task(retries=2, retry_delay=timedelta(minutes=1))
    def extract_binance_ohlcv() -> List[Dict]:
        url = f"https://api.binance.com/api/v3/klines?symbol={SYMBOL}&interval={INTERVAL}&limit={LIMIT}"
        logging.info(f"Fetching from: {url}")
        response = requests.get(url)
        response.raise_for_status()
        return response.json()

    @task()
    def transform_with_indicators(raw_data: List[Dict]) -> pd.DataFrame:
        df = pd.DataFrame(raw_data, columns=[
            "open_time", "open", "high", "low", "close", "volume",
            "close_time", "quote_asset_volume", "number_of_trades",
            "taker_buy_base_vol", "taker_buy_quote_vol", "ignore"
        ])

        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df = df[['open_time', 'open', 'high', 'low', 'close', 'volume']].astype(float)
        df['sma'] = ta.trend.sma_indicator(df['close'], window=14)
        df['ema'] = ta.trend.ema_indicator(df['close'], window=14)
        df['rsi'] = ta.momentum.rsi(df['close'], window=14)
        return df.dropna().tail(1)  # return latest complete row only

    @task()
    def load_to_postgres(df: pd.DataFrame):
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        engine = pg_hook.get_sqlalchemy_engine()
        df.to_sql("binance_ohlcv_indicators", engine, if_exists="append", index=False, method="multi")
        logging.info(f"Inserted {len(df)} rows into PostgreSQL")

    raw = extract_binance_ohlcv()
    enriched = transform_with_indicators(raw)
    load_to_postgres(enriched)

dag_instance = binance_5m_indicator_etl()
