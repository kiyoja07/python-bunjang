import pymysql
import pandas as pd
from config import SERVICE_1_CONFIG


def connect_to_service_1():

    try:
        connnection = pymysql.connect(host=SERVICE_1_CONFIG['host'],
                                      user=SERVICE_1_CONFIG['user'],
                                      password=SERVICE_1_CONFIG['password'],
                                      db=SERVICE_1_CONFIG['dbname'],
                                      charset='utf8',
                                      cursorclass=pymysql.cursors.DictCursor)
    except:
        print("Unable to connect to the database")

    return connnection

query = """
select * from notice limit 10
"""

df = pd.read_sql(query, connect_to_service_1())
print(df)

