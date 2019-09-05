import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from connect_db import connect_main_db
from service_2_query import *
from datetime import *
from dateutil.relativedelta import relativedelta
from save_query_result import save_query_result



def run_query_service_2_without_macro(query, save_path, db_name):

    query_count = 0
    query_params = None

    print('run at :', datetime.now().time())  # print start time

    result = connect_main_db(query, query_params, db_name)  # run query

    print(result)

    save_query_result(save_path, result, query_count)  # save to csv

    print('query completed')  # print end message

    return None



if __name__ == "__main__":

    print(query)

    db_name = 'SERVICE_2'

    try:

        run_query_service_2_without_macro(query, save_path, db_name)

        print('save_path : ', save_path)

    except ValueError as e:
        print(e)