import requests, json
import pymysql, sys, time
import sqlite3
from datetime import datetime

import logging

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
4, add nordpol (handle old api)

"""

# TODO
# Code gets new values all the time.. SOLVED
# Store to remote db
#   migrate to new db structure
# make it loop and create .service for host machine


# config
developing = s.settings()

if developing:
    logging.basicConfig(level=logging.INFO, filename="log.log", filemode="w",
                        format="%(asctime)s - %(levelname)s - %(message)s")
else:
    logging.basicConfig(level=logging.WARNING, filename="log.log", filemode="w",
                        format="%(asctime)s - %(levelname)s - %(message)s")


class Get_Data:
    """Get BTC price and store it to database"""

    def __init__(self, table: str) -> None:
        self.table = table
        self.data = {}
        self.sql_query = str

        self.get_data()

        #        # TODO
        #        print("store data at online db")

    def get_data(self):
        status = self.history()
        # check data. Get new ones?
        if status['old']:
            self.data = self.collect_data()
        if developing:
            self.print_data()

    def history(self):
        logging.debug("get history data")
        old_data = None
        conn_sqlite = sqlite3.connect("database.db")
        c3 = conn_sqlite.cursor()
        try:
            c3.execute("SELECT * FROM " + self.table + ";")
            old_data = c3.fetchone()
            conn_sqlite.commit()
            conn_sqlite.close()
            logging.info("found previous data (in local db)")
        except sqlite3.OperationalError:
            logging.exception("sqlite3 fetch error")

        hs = {'old': bool, 'ts': datetime, 'age': int}
        if old_data:
            ts = datetime.strptime(old_data[3], '%Y-%m-%d %H:%M:%S')

            hs['ts'] = ts
            dur = datetime.now() - ts
            hs['age'] = dur.total_seconds()
            hs['age_min'] = round(hs['age'] / 60)
            # check if data is older than 15 min and 3 sec, return True/False statement
            if hs['age'] > (15 * 60 + 3):
                hs['old'] = True
                logging.info("latest data is old, " + str(hs['age_min']) + " minutes old")
            else:
                hs['old'] = False
                logging.info("latest data not older than 15 min, " + str(hs['age_min']) + " minutes old")
                self.data = old_data
        else:
            hs['old'] = True
            hs['ts'] = None
            logging.info("No old data to be found")

        return hs

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
                self.table = "Bitcoin"
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

                logging.info("got new data from api")

                self.store_local(btc)
                # redundant?
                self.data = btc

                #if not developing:
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
        conn_sqlite = sqlite3.connect("database.db")
        c3 = conn_sqlite.cursor()
        c3.execute("DROP TABLE IF EXISTS " + self.table + ";")
        conn_sqlite.commit()
        c3.execute(str(self.sql_query))
        conn_sqlite.commit()

        # create next sql string
        self.sql_query = 'INSERT INTO ' + str(self.table) + ' (' + ', '.join(columns) + ') VALUES (' + (
                '?, ' * (len(columns) - 1)) + '?)'

        # print("DEBUG:", str(self.sql_query), tuple(values))

        c3.execute(str(self.sql_query), tuple(values))

        conn_sqlite.commit()
        if c3:
            conn_sqlite.close()
        logging.info("stored new data to local db")

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

            self.table = "Bitcoin"
            # create next sql string
            self.sql_query = 'INSERT INTO ' + str(self.table) + ' (' + ', '.join(columns) + ') VALUES (' + (
                    '%s, ' * (len(columns) - 1)) + '%s)'

            cursor.execute(str(self.sql_query), tuple(values))
            db.commit()
            db.close()
            logging.info("stored new data to remote db")
        except Exception as f:
            logging.exception("could not save to remote db:\n", str(f))

    def print_data(self):
        print("DEV: collected data from", self.table)
        print(".............start.............")
        if isinstance(self.data, dict):
            for d in self.data:
                print(d, ":", self.data[d], type(self.data[d]))
        else:
            print(self.data)
        print("..............end..............")


def start():
    print("program starts. Get BTC prices")
    try:
        Get_Data("Bitcoin")
    except Exception as e:
        print("could not launch, error:", e)
        logging.exception(e)
        # continue


if __name__ == "__main__":
    logging.info("Program started")
    if developing:
        logging.info("developer mode")
        print("developer mode")
    start()
    logging.info("Code completed")
