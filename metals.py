#import os.path
import os
import sys
import pickle
from datetime import datetime, timedelta
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


def fetch_from_api():
    params = {
        "api_key": API_KEY,
        "base": "USD",
        "currencies": "XAU,XAG"
    }
    response = requests.get(BASE_URL, params=params)
    response.raise_for_status()
    data = response.json()

    # Check for API errors
    if not data.get("success", True):
        print("❌ API error:", data)
        sys.exit(1)

    return data


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


def get_metal_data():
    cached = load_cache()
    if cached:
        print("✅ Using cached data")
        return cached

    print("🌐 Fetching fresh data from API")
    data = fetch_from_api()
    save_cache(data)
    return data


if __name__ == "__main__":
    log_path = os.path.join(BASE_DIR, "metals.log")
    if developing:
        logging.basicConfig(level=logging.DEBUG, filename=log_path, filemode="w",
                            format="%(asctime)s - %(levelname)s - %(message)s")
    else:
        logging.basicConfig(level=logging.WARNING, filename=log_path, filemode="w",
                            format="%(asctime)s - %(levelname)s - %(message)s")
    logging.info("metals.py stared standalone")

    metal_data = get_metal_data()
    print("📊 Full response:", metal_data)  # <- helpful for first run
    print("Base currency:", metal_data.get("base"))
    print("Rates:", metal_data.get("rates"))



#
# class GetPrice:
#     """Determine if a new value should be appended to database"""
#     def __init__(self) -> None:
#         """Api can only give 100 calls per month (3 a day)"""
#
#         logging.info("metals.GetPrice started")
#         self.data = {'ts_code': datetime.now()}
#         self.msg = str
#
#         # main loop
#         self.handle_data = True
#         while self.handle_data:
#             # got relevant data?
#             self.data['fresh'] = False
#             self.check_data()
#
#             # Need to get tomorrow's data?
#             if self.data['fresh']:
#                 self.handle_data = False
#
#             else:
#                 self.get_data()
#                 print("Refill remote db (NordPol)")
#
#         if developing:
#             print("Developing mode - Print data:")
#             for x in self.data:
#                 print(x, ":", self.data[x])
#         else:
#             pass
#
#         logging.info("End of electric stats")
#
#     def api_call(self):
#         params = {
#             "api_key": API_KEY,
#             "base": "USD",
#             "currencies": "XAU,XAG,WTI"  # Gold, Silver, Crude Oil
#         }
#         response = requests.get(BASE_URL, params=params)
#
#         if response.status_code == 200:
#             data = response.json()
#             print("Base Currency:", data["base"])
#             print("Timestamp:", data["timestamp"])
#             print("Rates:")
#             for metal, price in data["rates"].items():
#                 print(f"  {metal}: {price}")
#         else:
#             print("Error:", response.status_code, response.text)
#
#
#     def check_data(self):
#         self.data['db_data'] = None
#
#         try:
#             h, u, p, d = s.sql()
#             db = pymysql.connect(host=h, user=u, passwd=p, db=d)
#             cursor = db.cursor()
#
#             sql = "SELECT * FROM NordPool ORDER BY value_id DESC LIMIT 24"
#
#             cursor.execute(sql)
#             self.data['db_data'] = cursor.fetchall()
#             cursor.close()
#             logging.debug("Fetched the most recent data from database")
#         except Exception as f:
#             msg = f"could not get data from database. Error:\n{f}"
#             logging.exception(msg)
#             self.handle_data = False
#
#         if self.data['db_data']:
#
#             self.data['db_updated'] = self.data['db_data'][0][5]
#             self.data['db_lastrowid'] = self.data['db_data'][0][0]
#             # trigger for new data: time now +24 +2 hours, older than last updated record from database
#             self.data['deadline'] = self.data['db_updated'] + timedelta(days=1, hours=2)
#
#             # need to get new data?
#             if self.data['ts_code'] > self.data['deadline']:
#                 logging.info("Data is old. Time to get new data")
#                 self.data['fresh'] = False
#             else:
#                 logging.debug("Data still fresh in database")
#                 self.data['fresh'] = True
#
#         else:
#             logging.warning("has no data from database")
#             self.handle_data = False
#
#         return
#
#     def get_data(self):
#         self.data['api_call'] = None
#         try:
#             prices_spot = elspot.Prices(currency='SEK')
#             self.data['api_call'] = prices_spot.hourly(areas=['SE3'])
#             logging.info("Got data from online api")
#         except Exception as e:
#             msg = f"could not connect to NordPool server. Error:\n{e}"
#             logging.exception(msg)
#             print(msg)
#
#         if self.data['api_call']:
#             updated = self.data['api_call']['updated']
#             try:
#                 h, u, p, d = s.sql()
#                 db = pymysql.connect(host=h, user=u, passwd=p, db=d)
#                 cursor = db.cursor()
#
#                 v = self.data['api_call']['areas']['SE3']
#
#                 for x in v['values']:
#                     columns = []
#                     values = []
#                     for y in x:
#                         columns.append(y)
#                         values.append(x[y])
#                     columns.append('updated')
#                     values.append(updated)
#
#                     sql = 'INSERT INTO NordPool (' + ', '.join(columns) + ') VALUES (' + (
#                             '%s, ' * (len(columns) - 1)) + '%s)'
#
#                     cursor.execute(str(sql), tuple(values))
#                 db.commit()
#
#                 self.data['lastrowid'] = cursor.lastrowid
#                 db.close()
#
#                 print("End. Last row id:", self.data['lastrowid'])
#
#                 # if not receipt or new rows don´t add up, raise exception (compare last row with receipt)
#                 if self.data['lastrowid'] == self.data['db_lastrowid'] + 24:
#                     logging.info("New values are added to database")
#                 else:
#                     logging.exception("Added values do not match. Check database entries (did not add 24 rows)")
#                     raise ValueError("Did not add 24 rows")
#
#             except ValueError as e:
#                 logging.exception(f"ValueError:\n{e}")
#                 self.handle_data = False
#                 sys.exit()
#
#             except pymysql.Error as e:
#                 logging.exception(f"Error storing data to database:\n{e}")
#                 self.handle_data = False
#             return
#         else:
#             logging.warning("Has no data to save.. Exit")
#             self.handle_data = False
#         return
#
#
# if __name__ == "__main__":
#     log_path = os.path.join(BASE_DIR, "metals.log")
#     if developing:
#         logging.basicConfig(level=logging.DEBUG, filename=log_path, filemode="w",
#                             format="%(asctime)s - %(levelname)s - %(message)s")
#     else:
#         logging.basicConfig(level=logging.WARNING, filename=log_path, filemode="w",
#                             format="%(asctime)s - %(levelname)s - %(message)s")
#     logging.info("metals.py stared standalone")
#
#     GetPrice()
