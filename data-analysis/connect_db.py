#-*-coding:utf-8

import pymysql
import psycopg2
import pandas as pd
from config import REDSHIFT_CONFIG, SERVICE_1_CONFIG


# POSTGRESQL

def connect_redshift(query, query_params):

    product_connection_string = "dbname={dbname} user={user} host={host} password={password} port={port}"\
                                .format(dbname=REDSHIFT_CONFIG['dbname'],
                                        user=REDSHIFT_CONFIG['user'],
                                        host=REDSHIFT_CONFIG['host'],
                                        password=REDSHIFT_CONFIG['password'],
                                        port=REDSHIFT_CONFIG['port'],)
    try:
        product = psycopg2.connect(product_connection_string)

        result = pd.read_sql(query, product, params=query_params)

    # except:
    #     print("Unable to connect to the database")


    finally:
        product.close()


    return result


def connect_redshift_without_macro(query):

    product_connection_string = "dbname={dbname} user={user} host={host} password={password} port={port}"\
                                .format(dbname=REDSHIFT_CONFIG['dbname'],
                                        user=REDSHIFT_CONFIG['user'],
                                        host=REDSHIFT_CONFIG['host'],
                                        password=REDSHIFT_CONFIG['password'],
                                        port=REDSHIFT_CONFIG['port'],)
    try:
        product = psycopg2.connect(product_connection_string)

        result = pd.read_sql(query, product)

    # except:
    #     print("Unable to connect to the database")


    finally:
        product.close()


    return result


# MYSQL

def connect_service_1(query, query_params):

    try:
        connection = pymysql.connect(host=SERVICE_1_CONFIG['host'],
                                     user=SERVICE_1_CONFIG['user'],
                                     password=SERVICE_1_CONFIG['password'],
                                     db=SERVICE_1_CONFIG['dbname'],
                                     charset='utf8',
                                     cursorclass=pymysql.cursors.DictCursor)  # DB를 조회한 결과를 column 명이 key 인 dictionary로 저장

        result = pd.read_sql(query, connection, params=query_params)

    # except:
    #     print("Unable to connect to the database")


    finally:
        connection.close()

    return result


def connect_service_1_without_macro(query):

    try:
        connection = pymysql.connect(host=SERVICE_1_CONFIG['host'],
                                     user=SERVICE_1_CONFIG['user'],
                                     password=SERVICE_1_CONFIG['password'],
                                     db=SERVICE_1_CONFIG['dbname'],
                                     charset='utf8',
                                     cursorclass=pymysql.cursors.DictCursor)

        result = pd.read_sql(query, connection)

    # except:
    #     print("Unable to connect to the database")


    finally:
        connection.close()


    return result