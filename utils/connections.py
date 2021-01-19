import logging
import datetime
import sqlalchemy as sa


class DbConnect:
    def __init__(self, driver, host, port, username, password, database):
        self.driver = driver
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.database = database

    def create_sql_engine(self):
        # dialect[+driver]: // user: password @ host / dbname
        try :
            db_url = self.driver + '://' + self.username + ':' + self.password + '@' + self.host + '/' + self.database
            conn = sa.create_engine(db_url)
            logging.info('{} : Database connected successfully!'.format(
                datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')))
            return conn
        except :
            logging.error("{} : Was not able to connect to database!".format(
                datetime.datetime.strftime(datetime.datetime.now(), '%Y-%m-%d %H:%M:%S')))
            exit()
