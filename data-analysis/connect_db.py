#-*-coding:utf-8

import pymysql
import psycopg2
import pandas as pd
from config import REDSHIFT_CONFIG, SERVICE_1_CONFIG, SERVICE_2_CONFIG, QUICKET_LOG_CONFIG

#
# class ConnectSql:
#
#     def __init__(self, sql_type, query, params):
#         self.sql_type = sql_type
#         self.query = query
#         self.params = params
#
#     def connect_info(self):
#
#         if sql_type == 'POSTGRESQL':
#             dbname = REDSHIFT_CONFIG['dbname']
#             user = REDSHIFT_CONFIG['user']
#             host = REDSHIFT_CONFIG['host']
#             password = REDSHIFT_CONFIG['password']
#             port = REDSHIFT_CONFIG['port']
#
#             connection_info = {'dbname': dbname, 'user': user, 'host': host, 'password': password, 'port': port}
#
#             return connection_info
#
#     def read_query(self, product):
#
#         if params is None:
#             result = pd.read_sql(self.query, product)
#         else:
#             result = pd.read_sql(self.query, product, self.params)
#
#         return result
#
#
# query = None
# params = None
#
# sql_type = 'POSTGRESQL'
# config_info = ConnectSql.connect_info(sql_type)
#
# print(REDSHIFT_CONFIG['dbname'])
# print(config_info)
# print(config_info['dbname'])





#
#
# def connect_redshift(query, query_params):
# """ Redshift 연결 """
#
#     config_info = ConnectSql.connect_info(sql_type)
#
#     try:
#         product_connection_string = "dbname={dbname} user={user} host={host} password={password} port={port}" \
#                                     .format(dbname=config_info.get('dbname'),
#                                             user=REDSHIFT_CONFIG['user'],
#                                             host=REDSHIFT_CONFIG['host'],
#                                             password=REDSHIFT_CONFIG['password'],
#                                             port=REDSHIFT_CONFIG['port'], )
#
#         product = psycopg2.connect(product_connection_string)
#
#         ConnectSql.read_query(product)
#
#     finally:
#         product.close()
#
#     return result
#




# ------------------------------------------------------------



def read_query(query, connection, query_params):

    if query_params is None:
        query_result = pd.read_sql(query, connection)
    else:
        query_result = pd.read_sql(query, connection, params=query_params)

    return query_result


# POSTGRESQL

def connect_redshift(query, query_params):
# """ Redshift 연결 """

    try:
        connection = psycopg2.connect(dbname=REDSHIFT_CONFIG['dbname'],
                                      user=REDSHIFT_CONFIG['user'],
                                      host=REDSHIFT_CONFIG['host'],
                                      password=REDSHIFT_CONFIG['password'],
                                      port=REDSHIFT_CONFIG['port'])

        result = read_query(query, connection, query_params)

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


