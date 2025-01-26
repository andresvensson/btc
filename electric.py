import os.path
import sys
from datetime import datetime, timedelta
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
    """Determine if new electric prices should be appended to database"""
    def __init__(self) -> None:
        logging.info("electric.GetSpot started")
        self.data = {'ts_code': datetime.now()}
        self.msg = str

        # main loop
        self.handle_data = True
        while self.handle_data:
            # got relevant data?
            self.data['fresh'] = False
            self.check_data()

            # Need to get tomorrow's data?
            if self.data['fresh']:
                self.handle_data = False

            else:
                self.get_data()
                print("Refill remote db")

        if developing:
            print("Developing mode - Print data:")
            for x in self.data:
                print(x, ":", self.data[x])
        else:
            pass

        logging.info("End of electric stats")

    def check_data(self):
        self.data['db_data'] = None

        try:
            h, u, p, d = s.sql()
            db = pymysql.connect(host=h, user=u, passwd=p, db=d)
            cursor = db.cursor()

            sql = "SELECT * FROM NordPool ORDER BY value_id DESC LIMIT 24"

            cursor.execute(sql)
            self.data['db_data'] = cursor.fetchall()
            cursor.close()
            logging.info("Fetched the most recent data from database")
        except Exception as f:
            msg = "could not get data from database. Error:\n{0}".format(f)
            logging.exception(msg)
            self.handle_data = False

        if self.data['db_data']:

            self.data['db_updated'] = self.data['db_data'][0][5]
            self.data['db_lastrowid'] = self.data['db_data'][0][0]
            # trigger for new data: time now +24 +2 hours, older than last updated record from database
            self.data['deadline'] = self.data['db_updated'] + timedelta(days=1, hours=2)

            # need to get new data?
            if self.data['db_updated'] > self.data['deadline']:
                logging.info("Data is old. Time to get new data")
                self.data['fresh'] = False
            else:
                logging.info("Data still fresh in database")
                self.data['fresh'] = True

        else:
            logging.warning("has no data from database")
            self.handle_data = False

        return

    def get_data(self):
        self.data['api_call'] = None
        try:
            prices_spot = elspot.Prices(currency='SEK')
            self.data['api_call'] = prices_spot.hourly(areas=['SE3'])
            logging.info("Got data from online api")
        except Exception as e:
            msg = f"could not connect to NordPool server. Error:\n{e}"
            logging.exception(msg)
            print(msg)

        if self.data['api_call']:
            updated = self.data['api_call']['updated']
            try:
                h, u, p, d = s.sql()
                db = pymysql.connect(host=h, user=u, passwd=p, db=d)
                cursor = db.cursor()

                v = self.data['api_call']['areas']['SE3']

                for x in v['values']:
                    columns = []
                    values = []
                    for y in x:
                        columns.append(y)
                        values.append(x[y])
                    columns.append('updated')
                    values.append(updated)

                    sql = 'INSERT INTO NordPool (' + ', '.join(columns) + ') VALUES (' + (
                            '%s, ' * (len(columns) - 1)) + '%s)'

                    cursor.execute(str(sql), tuple(values))
                db.commit()

                self.data['lastrowid'] = cursor.lastrowid
                db.close()

                print("End. Last row id:", self.data['lastrowid'])

                # if not receipt or new rows don´t add up, raise exception (compare last row with receipt)
                if self.data['lastrowid'] == self.data['db_lastrowid'] + 24:
                    logging.info("New values are added to database")
                else:
                    logging.exception("Added values do not match. Check database entries (did not add 24 rows)")
                    raise ValueError("Did not add 24 rows")

            except ValueError as e:
                logging.exception(f"ValueError:\n{e}")
                self.handle_data = False
                sys.exit()

            except pymysql.Error as e:
                logging.exception(f"Error storing data to database:\n{e}")
                self.handle_data = False
            return
        else:
            logging.warning("Has no data to save.. Exit")
            self.handle_data = False
        return


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
