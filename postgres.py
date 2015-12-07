# Enthusiasts, 2015

import psycopg2


class PostgresInjection(object):

    def __init__(self, db_config):
        if PostgresInjection.__is_db_config_valid(db_config):
            self.__db_config = db_config
        else:
            raise ValueError("Database config is not proper.")

    def connection(self):
        return psycopg2.connect(host=self.__db_config["host"],
                                port=self.__db_config["port"],
                                database=self.__db_config["database"],
                                user=self.__db_config["user"],
                                password=self.__db_config["password"])

    @staticmethod
    def __is_db_config_valid(db_config):
        return db_config["host"] and db_config["port"] and ["user"] and db_config["password"] \
               and db_config["database"]