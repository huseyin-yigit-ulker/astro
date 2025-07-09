import logging
from datetime import datetime
from typing import List

import requests
from airflow.sdk import dag, task

SYMBOL = "BTCUSDT"
INTERVAL = "1m"
LIMIT = 5  # number of candles to fetch

@dag()
def binance_1m_print():

    @task(retries=2)
    def fetch_1m_data() -> List[List]:
        url = f"https://api.binance.com/api/v3/klines?symbol={SYMBOL}&interval={INTERVAL}&limit={LIMIT}"
        logging.info(f"Fetching 1m data from: {url}")
        response = requests.get(url)
        response.raise_for_status()
        data = response.json()
        return data

    @task
    def print_data(data: List[List]):
        for candle in data:
            open_time = candle[0]
            open_price = candle[1]
            close_price = candle[4]
            logging.info(f"Candle Open Time: {open_time}, Open: {open_price}, Close: {close_price}")

    raw_data = fetch_1m_data()
    print_data(raw_data)


binance_1m_print()
