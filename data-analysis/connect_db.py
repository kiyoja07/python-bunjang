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
    except:
        print("Unable to connect to the database")

    result = pd.read_sql(query, product, params=query_params)

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
    except:
        print("Unable to connect to the database")

    result = pd.read_sql(query, product)

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
                                     cursorclass=pymysql.cursors.DictCursor)

    except:
        print("Unable to connect to the database")

    result = pd.read_sql(query, connection, params=query_params)

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

    except:
        print("Unable to connect to the database")

    result = pd.read_sql(query, connection)

    connection.close()

    return result