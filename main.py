import logging
import os.path
import sqlite3
from datetime import datetime, timedelta
import math
import json
import pymysql
import requests
import sys
import time

import electric
# import local settings and personal info
import secret as s

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
    logging.basicConfig(level=logging.WARNING, filename="log.log", filemode="w",
                        format="%(asctime)s - %(levelname)s - %(message)s")


class Get_Data:
    """Get BTC price and store it to database"""

    def __init__(self) -> None:
        self.sleep = 10
        self.data = {}
        self.sql_query = str
        # self.sleep: float
        self.loop_code = True

        # NordPool data
        electric.GetSpot()

        logging.info("main.py continues")
        self.get_data()

        if developing:
            self.print_data()
            self.loop_code = False

        while self.loop_code:
            # make sure time in sync
            time.sleep(5)
            # get eta
            self.set_sleep()
            # wait for next 15 min step
            time.sleep(self.sleep)
            self.collect_data()
            electric.GetSpot()

    def get_data(self):
        logging.debug("get history data")
        old_data = None
        conn_sqlite = sqlite3.connect(db_path)
        c3 = conn_sqlite.cursor()
        try:
            c3.execute("SELECT * FROM Bitcoin;")
            old_data = c3.fetchone()
            conn_sqlite.commit()
            conn_sqlite.close()
        except sqlite3.OperationalError:
            logging.exception("sqlite3 fetch error")

        hs = {'old': bool, 'ts': datetime, 'age': int}
        if old_data:
            logging.info("Found previous data (in local db)")
            ts = datetime.strptime(old_data[3], '%Y-%m-%d %H:%M:%S')
            hs['ts'] = datetime.strptime(old_data[3], '%Y-%m-%d %H:%M:%S')
            dur = datetime.now() - ts
            hs['age'] = dur.total_seconds()
            hs['age_min'] = round(hs['age'] / 60)
            msg = "Latest data from {0}, ({1} minutes old).".format(hs['ts'], hs['age_min'])
            logging.info(msg)
            self.data = old_data
        else:
            self.collect_data()

        return

    def round_dt(self, dt):
        # round upwards to nearest 15 min
        delta = timedelta(minutes=15)
        return datetime.min + math.ceil((dt - datetime.min) / delta) * delta

    def set_sleep(self):
        # calc seconds left to get new data
        ts_next = self.round_dt(datetime.now())
        dur = ts_next - datetime.now()
        self.sleep = dur.total_seconds()

    def collect_data(self):
        btc = {}
        btc_attempts = 0
        while btc_attempts <= 3:
            try:
                r = requests.get(s.url_btc())
                raw_data = json.loads(r.text)
                # Price and Time as of old db structure
                btc['Price'] = float(raw_data['last'])

                ts = raw_data['timestamp']
                time_stamp = datetime.fromtimestamp(int(ts))
                btc['Time'] = time_stamp

                # add describable info
                btc['info'] = "BitCoin price"
                btc['source'] = s.url_btc()

                # prepare sql string
                self.sql_query = ("CREATE TABLE IF NOT EXISTS Bitcoin (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, "
                                  "db_ts CURRENT_TIMESTAMP, Price REAL, Time TEXT, "
                                  "info TEXT, source TEXT, ")

                # add whole api call
                for r in raw_data:
                    btc[str(r)] = raw_data[r]
                    if isinstance(raw_data[r], float):
                        self.sql_query = self.sql_query + str(r) + " REAL, "
                    else:
                        self.sql_query = self.sql_query + str(r) + " TEXT, "

                self.sql_query = self.sql_query[0:-2] + ");"

                logging.info("Got new data from api")

                self.data = btc
                self.store_local(btc)
                self.store_remote()

                return btc

            except requests.ConnectionError as f:
                text = str(btc_attempts) + " attempt of 3" + "API Error: " + str(f)
                logging.error(text)
                btc = {}
                time.sleep(5)
                btc_attempts += 1

        if btc_attempts > 3:
            text = "Could not get any values... Exit"
            logging.error(text)
            sys.exit()

        return btc

    def store_local(self, btc):
        # loop data to prepare sql string construction later
        columns = []
        values = []
        for x in btc:
            columns.append(x)
            values.append(btc[x])

        # prepare db with sql string
        conn_sqlite = sqlite3.connect(db_path)
        c3 = conn_sqlite.cursor()
        c3.execute("DROP TABLE IF EXISTS Bitcoin;")
        conn_sqlite.commit()
        c3.execute(str(self.sql_query))
        conn_sqlite.commit()

        # create next sql string
        self.sql_query = 'INSERT INTO Bitcoin (' + ', '.join(columns) + ') VALUES (' + (
                '?, ' * (len(columns) - 1)) + '?)'

        # print("DEBUG:", str(self.sql_query), tuple(values))

        c3.execute(str(self.sql_query), tuple(values))

        conn_sqlite.commit()
        if c3:
            conn_sqlite.close()
        logging.info("Stored new data to local db")

    def store_remote(self):
        try:
            columns = []
            values = []
            for x in self.data:
                columns.append(x)
                values.append(self.data[x])

            h, u, p, d = s.sql()
            db = pymysql.connect(host=h, user=u, passwd=p, db=d)
            cursor = db.cursor()

            if developing:
                # TODO, SQL!, before deployment, change to Bitcoin_dev instead
                table = "Bitcoin_dev"
            else:
                table = "Bitcoin"
            # create next sql string
            self.sql_query = 'INSERT INTO ' + str(table) + ' (' + ', '.join(columns) + ') VALUES (' + (
                    '%s, ' * (len(columns) - 1)) + '%s)'

            cursor.execute(str(self.sql_query), tuple(values))
            db.commit()
            db.close()
            logging.info("stored new data to remote db")
        except Exception as f:
            msg = "could not save to remote db:\n{0}".format(f)
            logging.exception(msg)
        # next run?

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
        print("--developer mode--")
    Get_Data()
    logging.info("main.py completed")
