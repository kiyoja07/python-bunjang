#-*-coding:utf-8

import pymysql
from psycopg2 import connect
import pandas as pd
from config import REDSHIFT_CONFIG, SERVICE_1_CONFIG, SERVICE_2_CONFIG, QUICKET_LOG_CONFIG


# class sql -> class postgresql, mysql

class ConnectSql():

    def __init__(self, db_name):
        self.db_name = db_name

    def connect_info(self):

        if self.db_name == 'REDSHIFT':
            db_config = REDSHIFT_CONFIG
        elif self.db_name == 'SERVICE_1':
            db_config = SERVICE_1_CONFIG
        elif self.db_name == 'SERVICE_2':
            db_config = SERVICE_2_CONFIG
        elif self.db_name == 'QUICKET_LOG':
            db_config = QUICKET_LOG_CONFIG

        dbname = db_config['dbname']
        user = db_config['user']
        host = db_config['host']
        password = db_config['password']
        port = db_config['port']

        product_connection_string = "dbname={dbname} user={user} host={host} password={password} port={port}" \
                                    .format(dbname=dbname, user=user, host=host, password=password, port=port)

        return product_connection_string


def read_query(query, connection, query_params):

    if query_params is None:
        query_result = pd.read_sql(query, connection)
    else:
        query_result = pd.read_sql(query, connection, params=query_params)

    return query_result


# POSTGRESQL

def connect_redshift(query, query_params, db_name):

    connection_string = ConnectSql(db_name).connect_info()

    try:
        connection = connect(connection_string)

        result = read_query(query, connection, query_params)

    except Exception as e:
        print(e)

    finally:
        connection.close()

    return result




# MYSQL

def connect_service_1(query, query_params):
# """ Service 1 연결 """

    try:
        connection = pymysql.connect(host=SERVICE_1_CONFIG['host'],
                                     user=SERVICE_1_CONFIG['user'],
                                     password=SERVICE_1_CONFIG['password'],
                                     db=SERVICE_1_CONFIG['dbname'],
                                     charset='utf8',
                                     cursorclass=pymysql.cursors.DictCursor)  # DB를 조회한 결과를 column 명이 key 인 dictionary로 저장

        result = read_query(query, connection, query_params)

    finally:
        connection.close()

    return result


def connect_service_2_without_macro(query):
# """ Service 2 연결 """

    try:
        connection = pymysql.connect(host=SERVICE_2_CONFIG['host'],
                                     user=SERVICE_2_CONFIG['user'],
                                     password=SERVICE_2_CONFIG['password'],
                                     db=SERVICE_2_CONFIG['dbname'],
                                     charset='utf8',
                                     cursorclass=pymysql.cursors.DictCursor)

        result = read_query(query, connection, query_params)

    finally:
        connection.close()

    return result


def connect_quicket_log(query, query_params):
# """ quicket log 연결 """

    try:
        connection = pymysql.connect(host=QUICKET_LOG_CONFIG['host'],
                                     user=QUICKET_LOG_CONFIG['user'],
                                     password=QUICKET_LOG_CONFIG['password'],
                                     db=QUICKET_LOG_CONFIG['dbname'],
                                     charset='utf8',
                                     cursorclass=pymysql.cursors.DictCursor)

        result = read_query(query, connection, query_params)

    finally:
        connection.close()

    return result

