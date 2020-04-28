#-*-coding:utf-8

import pymysql
import psycopg2
import pandas as pd
from config import *


class FindSqlConfig:

    def __init__(self, db_name):
        self.db_name = db_name

    def find_db_config(self):
        if self.db_name == 'REDSHIFT':
            db_config = REDSHIFT_CONFIG
        elif self.db_name == 'SERVICE_1':
            db_config = SERVICE_1_CONFIG
        elif self.db_name == 'SERVICE_2':
            db_config = SERVICE_2_CONFIG
        elif self.db_name == 'QUICKET_LOG':
            db_config = QUICKET_LOG_CONFIG
        elif self.db_name == 'BUN_DW':
            db_config = BUN_DW_CONFIG

        dbname = db_config['dbname']
        user = db_config['user']
        host = db_config['host']
        password = db_config['password']
        port = db_config['port']

        config = {'dbname': dbname, 'user': user, 'host': host, 'password': password, 'port': port}

        return config

class ConnectPostgresql(FindSqlConfig):

    def __init__(self, db_name):
        super().__init__(db_name)

    def connect_sql(self):
        config = FindSqlConfig(self.db_name).find_db_config()

        connection = psycopg2.connect(dbname=config['dbname'],
                                      host=config['host'],
                                      user=config['user'],
                                      password=config['password'],
                                      port=config['port'])

        return connection


class ConnectMysql(FindSqlConfig):

    def __init__(self, db_name):
        super().__init__(db_name)

    def connect_sql(self):
        config = FindSqlConfig(self.db_name).find_db_config()

        connection = pymysql.connect(host=config['host'],
                                     user=config['user'],
                                     password=config['password'],
                                     db=config['dbname'],
                                     charset='utf8',
                                     cursorclass=pymysql.cursors.DictCursor)  # DB를 조회한 결과를 column 명이 key 인 dictionary로 저장

        return connection


def read_query(query, connection, query_params):

    if query_params:
        query_result = pd.read_sql(query, connection, params=query_params)
    else:
        query_result = pd.read_sql(query, connection)

    return query_result


def connect_redshift(query, db_name, query_params=None):

    try:
        connection = ConnectPostgresql(db_name).connect_sql()

        result = read_query(query, connection, query_params)

    except Exception as e:
        print(e)

    finally:
        connection.close()

    return result


def connect_postgresql(query, db_name, query_params=None):

    try:
        connection = ConnectPostgresql(db_name).connect_sql()

        result = read_query(query, connection, query_params)

    except Exception as e:
        print(e)

    finally:
        connection.close()

    return result



def connect_main_db(query, query_params, db_name):

    try:
        connection = ConnectMysql(db_name).connect_sql()

        result = read_query(query, connection, query_params)

    except Exception as e:
        print(e)

    finally:
        connection.close()

    return result




# def connect_quicket_log(query, query_params):
# # """ quicket log 연결 """
#
#     try:
#         connection = pymysql.connect(host=QUICKET_LOG_CONFIG['host'],
#                                      user=QUICKET_LOG_CONFIG['user'],
#                                      password=QUICKET_LOG_CONFIG['password'],
#                                      db=QUICKET_LOG_CONFIG['dbname'],
#                                      charset='utf8',
#                                      cursorclass=pymysql.cursors.DictCursor)
#
#         result = read_query(query, connection, query_params)
#
#     finally:
#         connection.close()
#
#     return result


if __name__ == '__main__':

    # test code

    db_name = 'REDSHIFT'

    db_name ='BUN_DW'

    connection = ConnectPostgresql(db_name).connect_sql()

    connection.close()

    print(connection)
