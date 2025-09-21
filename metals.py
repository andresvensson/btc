import os
import sys
import pickle
from datetime import datetime, timezone
import pymysql
import logging
import requests
import time
import secret as s


# CONFIG

# in case of stand-alone run
developing = s.settings()
# path for local database
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

API_KEY = s.metals("api")
BASE_URL = "https://api.metalpriceapi.com/v1/latest"
CACHE_FILE = "metal_cache.pkl"
CACHE_TTL = 30600  # cache 8,5 hours minutes
# MariaDB connection settings
h, u, p, d = s.sql()
DB_CONFIG = {
    "host": h,
    "user": u,
    "password": p,
    "database": d,
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

def run():
    metals = ["XAU", "XAG"]  # Gold & Silver
    prices = get_prices(metals)
    save_to_mariadb(prices)


def load_cache():
    if os.path.exists(CACHE_FILE):
        with open(CACHE_FILE, "rb") as f:
            cache = pickle.load(f)
            if time.time() - cache["timestamp"] < CACHE_TTL:
                return cache["data"]
    return None


def save_cache(data):
    with open(CACHE_FILE, "wb") as f:
        pickle.dump({"timestamp": time.time(), "data": data}, f)


def fetch_from_api(currencies):
    params = {
        "api_key": API_KEY,
        "base": "USD",
        "currencies": ",".join(currencies)
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()

    if not data.get("success", True):
        print("❌ API error:", data)
        logging.error(f"API error, data {data}")
        sys.exit(1)

    return data


def get_prices(currencies):
    cached = load_cache()
    if cached:
        # print("✅ Using cached data")
        return cached

    print("🌐 Fetching fresh data from API")
    logging.info("Fetching fresh data from API")
    data = fetch_from_api(currencies)
    save_cache(data)
    return data


def save_to_mariadb(price_data):
    conn = pymysql.connect(**DB_CONFIG)
    try:
        with conn.cursor() as cursor:
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS metal_prices (
                    id INT AUTO_INCREMENT PRIMARY KEY,
                    timestamp DATETIME,
                    base VARCHAR(10),
                    metal VARCHAR(10),
                    rate DOUBLE
                )
            """)

            ts = datetime.now(timezone.utc)
            base = price_data.get("base", "USD")
            rates = price_data.get("rates", {})

            for metal, rate in rates.items():
                cursor.execute(
                    "SELECT rate FROM metal_prices WHERE metal=%s ORDER BY timestamp DESC LIMIT 1",
                    (metal,)
                )
                row = cursor.fetchone()

                if not row or row["rate"] != rate:
                    cursor.execute(
                        "INSERT INTO metal_prices (timestamp, base, metal, rate) VALUES (%s, %s, %s, %s)",
                        (ts, base, metal, rate)
                    )
                    # print(f"💾 Saved {metal}: {rate}")
                    logging.info(f"💾 Saved {metal}: {rate}")
                else:
                    # print(f"⏩ Skipped {metal}, no change")
                    logging.info(f"⏩ Skipped {metal}, no change")

        conn.commit()
    finally:
        conn.close()


if __name__ == "__main__":
    log_path = os.path.join(BASE_DIR, "metals.log")
    if developing:
        logging.basicConfig(level=logging.DEBUG, filename=log_path, filemode="w",
                            format="%(asctime)s - %(levelname)s - %(message)s")
    else:
        logging.basicConfig(level=logging.WARNING, filename=log_path, filemode="w",
                            format="%(asctime)s - %(levelname)s - %(message)s")
    logging.info("metals.py stared standalone")

    run()
