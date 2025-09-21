import logging
import os.path
from datetime import datetime, timedelta
import math
import json
from dbm import error
from logging import exception

import pymysql
import requests
import time

import electric
import metals
# import local settings and personal info
import secret as s

# Modded 2025-01-27
#   code cleanup -sqlite3, code rewrite adjusting multiple records in db. Make code loop and service for host
# Modded 2024-02-18
# Modded 2021-05-02
# Created 2018-08-18

"""
# PLAN
1, mod code so it can be added in git *CHECK*
2, add currency rates (have old code)
3, add aktiekurser (find api)
4, add nordpol (handle old api) *CHECK*

"""

# TODO
# Code gets new values all the time.. SOLVED
# Store to remote db
#   create table if not exists
# read in logger. Filename inconsistency. Append? Empty log on runtime?

# TODO create a service for host

# config
developing = s.settings()

# path for local database
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(BASE_DIR, "database.db")
log_path = os.path.join(BASE_DIR, "log.log")

if developing:
    logging.basicConfig(level=logging.DEBUG, filename=log_path, filemode="w",
                        format="%(asctime)s - %(levelname)s - %(message)s")
else:
    logging.basicConfig(level=logging.INFO, filename="log.log", filemode="w",
                        format="%(asctime)s - %(levelname)s - %(message)s")


def round_dt(dt):
    # round upwards to nearest 15 min
    delta = timedelta(minutes=15)
    return datetime.min + math.ceil((dt - datetime.min) / delta) * delta


class Get_Data:
    """Get BTC price and store it to database"""

    def __init__(self) -> None:
        self.sleep = 10
        self.data = {}

        self.sql_query = str
        # self.sleep: float
        self.loop_code = True

        while self.loop_code:
            # NordPool data
            try:
                electric.GetSpot()
            except error:
                continue

            # Metals data
            try:
                metals.run()
            except error:
                continue

            logging.info("main.py continues")
            self.collect_data()

            if developing:
                self.print_data()
                break

            # make sure time in sync
            time.sleep(5)
            # get eta
            self.set_sleep()
            # wait for next 15 min step
            time.sleep(self.sleep)

    def set_sleep(self):
        # calc seconds left to get new data
        ts_next = round_dt(datetime.now())
        dur = ts_next - datetime.now()
        self.sleep = dur.total_seconds()

    def collect_data(self):
        btc = {}
        try:
            r = requests.get(s.url_btc())
            raw_data = json.loads(r.text)

            self.data['raw_data'] = raw_data
            # Price and Time as of old db structure
            ts = raw_data['timestamp']
            time_stamp = datetime.fromtimestamp(int(ts))
            btc['Time'] = time_stamp
            btc['Price'] = float(raw_data['last'])

            # add describable info
            btc['info'] = "BitCoin price"
            btc['source'] = s.url_btc()

            logging.debug("Got new data from api")

            self.data['sql'] = btc
            self.store_remote()

        except requests.ConnectionError as f:
            logging.exception(f"Could not save values to database. Error:\n{f}")

        return

    def store_remote(self):
        try:
            columns = []
            values = []
            for x in self.data['sql']:
                columns.append(x)
                values.append(self.data['sql'][x])

            for rd in self.data['raw_data']:
                columns.append(rd)
                values.append(self.data['raw_data'][rd])

            h, u, p, d = s.sql()
            db = pymysql.connect(host=h, user=u, passwd=p, db=d)
            cursor = db.cursor()
            # create sql string
            sql_query = 'INSERT INTO Bitcoin (' + ', '.join(columns) + ') VALUES (' + (
                    '%s, ' * (len(columns) - 1)) + '%s)'

            cursor.execute(str(sql_query), tuple(values))
            db.commit()
            db.close()
            logging.info("stored new data to remote db")
        except Exception as f:
            msg = "could not save to remote db:\n{0}".format(f)
            logging.exception(msg)

    def print_data(self):
        print("\nDEV: Bitcoin data")
        print(".............start.............")
        if isinstance(self.data, dict):
            for d in self.data:
                print(d, ":", self.data[d], type(self.data[d]))
        else:
            if self.data:
                print(self.data)
            else:
                print("No data to show, next run in {0} min".format(round(self.sleep / 60)))
        print("..............end..............")


if __name__ == "__main__":
    logging.info("main.py started")
    if developing:
        logging.info("--in developer mode--")

    Get_Data()
    logging.debug("main.py completed")
