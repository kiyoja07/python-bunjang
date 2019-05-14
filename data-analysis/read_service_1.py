import pymysql
import pandas as pd
from config import SERVICE_1_CONFIG


def connect_to_service_1(query, query_params):

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

    return result
