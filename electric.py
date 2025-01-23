import os.path
import sqlite3
import sys
import pickle
import datetime
import pymysql
from nordpool import elspot
import logging

import secret as s


# in case of stand-alone run
developing = s.settings()
# path for local database
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
db_path = os.path.join(BASE_DIR, "database.db")


class GetSpot:
    def __init__(self) -> None:
        logging.info("electric.GetSpot started")
        self.data = None
        self.fresh = False
        self.sql_query = str

        self.get_data()
        logging.info("End of electric stats")

    def get_data(self):
        # check local db and local api file dump
        status = self.history()

        if status['old']:
            self.call_api()

        if developing:
            self.print_data()

        return

    def history(self):
        logging.info("Get locally saved data")
        hs = {'old': bool, 'ts': datetime, 'age': int, 'db_data': None}

        self.fresh = False
        source = 0

        try:
            conn_sqlite = sqlite3.connect(s.db_dir())
            c3 = conn_sqlite.cursor()
            c3.execute("SELECT * FROM electric;")
            db_data = c3.fetchall()
            hs['db_data'] = db_data
            conn_sqlite.commit()
            conn_sqlite.close()
            source = 1
            logging.info("Found previous data (in local db)")

        except sqlite3.OperationalError as e:
            print("sqlite3 fetch error:" + str(e))
            logging.warning("sqlite3 fetch error:" + str(e))

        if not hs['db_data']:
            logging.info("No data in local db, Try read api file dump")
            try:
                with open('api_data.pkl', 'rb') as f:
                    self.data = pickle.load(f)
                    hs['db_data'] = self.data
                    source = 2
                    logging.info("Found a saved api call in a local file")

            except Exception as e:
                logging.exception("Found no locally saved api file")
                print("No saved values i in local file", e)
                pass

        if hs['db_data']:
            if source == 1:
                ts = datetime.datetime.strptime(hs['db_data'][0][10], '%Y-%m-%d %H:%M:%S.%f+00:00')
            elif source == 2:
                ts = datetime.datetime.strptime(str(self.data['updated']), '%Y-%m-%d %H:%M:%S.%f+00:00')
            else:
                ts = None

            logging.debug("Date for latest data locally: {0}".format(ts))

            # calculate eta when to get data: Latest + 25 hrs
            eta = ts + datetime.timedelta(days=1, hours=1)
            logging.debug("ETA when to call api: {0}".format(eta))
            ts_now = datetime.datetime.now()

            hs['ts'] = ts
            dur = eta - ts_now
            hs['age'] = dur.total_seconds()

            hs['age_hrs'] = round((hs['age'] / 60) / 60)

            if ts_now > eta:
                hs['old'] = True
                msg = "Time to get new data ({0} hours)".format(str(hs['age_hrs']))
                logging.info(msg)

            else:
                hs['old'] = False
                msg = "No need to get data. {0} hours to next api call.".format(str(hs['age_hrs']))
                if source == 1:
                    self.data = hs['db_data']
                if source == 2:
                    self.fresh = True
                    receipt = self.store_local()
                    if receipt:
                        remote_receipt = self.store_remote()

                    else:
                        print("Could not get receipt from local store!")
                        logging.warning("Could not get receipt from local store")
                logging.info(msg)

        else:
            hs['old'] = True
            hs['ts'] = None
            logging.info("No local data to be found. Get new ones")

        return hs

    def call_api(self):
        try:
            if s.offline_mode():
                logging.info("Offline mode. Read local file")
                try:
                    with open('api_data.pkl', 'rb') as f:
                        self.data = pickle.load(f)
                        logging.info("Found a saved api call in local file")
                        pass

                except Exception as e:
                    logging.exception("Found no locally saved api file")
                    print("No saved values i in local file", e)
            elif not self.fresh:
                prices_spot = elspot.Prices(currency='SEK')
                self.data = prices_spot.hourly(areas=['SE3'])
                self.fresh = True

                logging.info("Got data from online api")

            # save data or not
            if self.fresh:
                if s.offline_mode():
                    # redundant remove pls
                    logging.info("Offline mode. Not saving same values")
                    pass
                else:
                    with open("api_data.pkl", "wb") as f:
                        pickle.dump(self.data, f)
                        logging.info("Dumped data to local file 'api_data.pkl'")

                    receipt = self.store_local()
                    if receipt:
                        remote_receipt = self.store_remote()
                        # If remote save fails, delete local db
                        if not remote_receipt:
                            conn_sqlite = sqlite3.connect(db_path)
                            c3 = conn_sqlite.cursor()
                            c3.execute("DROP TABLE IF EXISTS electric;")
                            conn_sqlite.commit()
                            conn_sqlite.close()
                        else:
                            pass
                    else:
                        print("Could not get receipt from local store!")
                        logging.warning("Could not get receipt from local store")
            else:
                logging.warning("Be aware. Data probably old")

        except Exception as e:
            msg = "Error in 'call_api', Error:\n{0}".format(e)
            logging.exception(msg)
            print(msg)

    def print_data(self):
        # TODO print saved api call from file if in offline mode?
        logging.info("Printing values to console")
        print("\nDEV: electric data")
        if self.data:
            if self.fresh:
                print("-----------data---------------")
                for r in self.data:
                    print("row:", r, ":", self.data[r])
                print("-----------data---------------")

                print("..........values..............")
                data_values = self.data['areas']['SE3']['values']
                for r in data_values:
                    print("row:", r['start'], ":", r['end'], ":", r['value'], ":")
                print("..........values..............")
                pass

            else:
                print("-----------data---------------")
                for r in self.data:
                    print(r, type(r))
                print("-----------data---------------")

        else:
            print("(from local db)")
            print("..........values..............")
            print("No data to show..")
            print("..........values..............")

    def store_local(self):
        """Create one table electric and store first stats about all values then store each hour on a row (1+24 rows)
        delete tables before insert new. This makes so code can fetch one (latest value/time) or fetch all
        also get stats"""
        conn_sqlite = sqlite3.connect(db_path)
        c3 = conn_sqlite.cursor()

        # I considered update statement but this deletes if table exists
        c3.execute("DROP TABLE IF EXISTS electric;")
        conn_sqlite.commit()

        # create new table
        sql = ("CREATE TABLE IF NOT EXISTS electric (id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, Average REAL, "
               "Min REAL, Max REAL, Peak REAL, OffPeak1 REAL, OffPeak2 REAL, start TEXT, end TEXT, value REAL, "
               "updated REAL)")
        c3.execute(sql)
        conn_sqlite.commit()

        # insert first row (summary data)
        sql = "INSERT INTO electric (Average, Min, Max, Peak, OffPeak1, OffPeak2, updated) VALUES (?, ?, ?, ?, ?, ?, ?)"
        v = self.data['areas']['SE3']
        #print("v:...............\n", v)
        #for x in v['values']:
        #    print(v['values'], x)
            #for y in x:
                #print(y)

        #TODO Breakpoint code needs to be re written..
        #sys.exit()

        val = (v['Average'], v['Min'], v['Max'], v['Peak'], v['Off-peak 1'], v['Off-peak 2'], self.data['updated'])
        c3.execute(sql, val)
        conn_sqlite.commit()

        # insert 24 hourly prices (+24 rows)
        for x in v['values']:
            columns = []
            values = []
            for y in x:
                columns.append(y)
                values.append(x[y])

            sql = 'INSERT INTO electric (' + ', '.join(columns) + ') VALUES (' + (
                    '?, ' * (len(columns) - 1)) + '?)'

            c3.execute(str(sql), tuple(values))
            conn_sqlite.commit()

        receipt = c3.lastrowid

        # close connection
        if c3:
            conn_sqlite.close()
        logging.info("Stored new data to local db")
        return receipt

    def store_remote(self):
        """check latest value on remote. Store new data from api call to remote database"""

        if developing:
            table = "NordPool_dev"
        else:
            table = "NordPool"

        last_row = {'ts': None, 'id': int}

        # get latest record
        h, u, p, d = s.sql()
        db = pymysql.connect(host=h, user=u, passwd=p, db=d)
        cursor = db.cursor()
        sql_query = "SELECT * FROM " + table + " ORDER BY id DESC LIMIT 25;"
        # check last value in remote db
        try:
            cursor.execute(str(sql_query))
            old_data = cursor.fetchall()
            db.close()

            # in case of saving to empty table
            if not old_data:
                last_row['ts'] = datetime.datetime.now()
                last_row['id'] = -24
            else:
                last_row['ts'] = old_data[24][11]
                last_row['id'] = old_data[24][0]

            dt1 = self.data['updated']
            ts1 = dt1.strftime('%Y-%m-%d %H:%M:%S')

            dt2 = last_row['ts']
            ts2 = dt2.strftime('%Y-%m-%d %H:%M:%S')

            # check if data already saved
            if ts2 == ts1:
                logging.exception("Value already saved, abort")
                raise ValueError("Value already saved in remote database")
            else:
                logging.info("Data has not been added before")
                pass

        except ValueError as e:
            msg = "Exit code!!! : {0}".format(e)
            print(msg)
            logging.exception(msg)
            return

        except pymysql.ProgrammingError as e:
            logging.exception("Missing table or values? Msg:{0}".format(e))
            pass

        except pymysql.Error as e:
            msg = "Error reading DB: {0}".format(e)
            print(msg)
            logging.exception(str(msg))
            sys.exit()
            # TODO fetch exception error when no db and add function so code can create db, use IF NOT EXIST?

        receipt = None
        try:
            h, u, p, d = s.sql()
            db = pymysql.connect(host=h, user=u, passwd=p, db=d)
            cursor = db.cursor()

            v = self.data['areas']['SE3']
            val = v['Average'], v['Min'], v['Max'], v['Peak'], v['Off-peak 1'], v['Off-peak 2'], self.data['updated']

            parsed_val = []
            parsed_column = []
            column_list = "Average", "Min", "Max", "Peak", "OffPeak1", "OffPeak2", "updated"

            # probably don't need this loop, just replace inf values
            c = 0
            for y in val:
                if y == float("inf"):
                    parsed_val.append(0)
                else:
                    parsed_val.append(y)
                parsed_column.append(column_list[c])
                c += 1
            val = parsed_val

            sql = 'INSERT INTO ' + str(table) + ' (' + ', '.join(parsed_column) + ') VALUES (' + (
                    '%s, ' * (len(parsed_val) - 1)) + '%s)'

            # print("DEBUG!:\n", sql, val)
            cursor.execute(sql, val)
            db.commit()

            for x in v['values']:
                columns = []
                values = []
                for y in x:
                    columns.append(y)
                    values.append(x[y])

                sql = 'INSERT INTO ' + str(table) + ' (' + ', '.join(columns) + ') VALUES (' + (
                        '%s, ' * (len(columns) - 1)) + '%s)'

                cursor.execute(str(sql), tuple(values))
            db.commit()
            receipt = cursor.lastrowid
            db.close()

            # if not receipt or new rows don´t add up, raise exception (compare last row with receipt)
            if last_row['id'] + 49 == receipt:
                logging.info("Stored new data to remote db")
            else:
                logging.exception("Added values does not match. Check remote db entries (did not add 25 rows)")
                raise ValueError("Did not add 25 rows")

        except ValueError as e:
            print("Exit code!!! :", e)
            sys.exit()

        except pymysql.Error as e:
            msg = "Error storing to DB:\n{0}".format(e)
            print(msg)
            logging.exception(msg)
        return receipt


if __name__ == "__main__":
    log_path = os.path.join(BASE_DIR, "electric.log")
    if developing:
        logging.basicConfig(level=logging.DEBUG, filename=log_path, filemode="w",
                            format="%(asctime)s - %(levelname)s - %(message)s")
    else:
        logging.basicConfig(level=logging.WARNING, filename=log_path, filemode="w",
                            format="%(asctime)s - %(levelname)s - %(message)s")
    logging.info("electric.py stared standalone")
    GetSpot()
