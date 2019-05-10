#-*-coding:utf-8

import psycopg2
import pandas as pd
from config import REDSHIFT_CONFIG


def connect_redshitf_old():

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

    return product


query = """
select updated, json_data
from pageview
where date(updated) = '2019-05-01'
"""

# df = pd.read_sql(query, connect_redshitf())
# df.to_csv()
#
# print(df)


def connect_redshift(query):

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

    df = pd.read_sql(query, product)

    return df


