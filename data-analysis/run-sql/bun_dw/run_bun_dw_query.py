import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from connect_db import connect_postgresql
from bun_dw_query import *
from datetime import datetime
from dateutil.relativedelta import relativedelta
from save_query_result import save_query_result


def run_query_bun_dw_without_macro(query, save_path, db_name):
    """ 매크로 없이 쿼리 실행 """

    print('run at :', datetime.now().time())  # print start time

    result = connect_postgresql(query, db_name)  # run query

    save_query_result(save_path, result)  # save to csv

    return None


def run_query_bun_dw_macro(query, save_path, db_name, start_time, end_time, interval_type):
    """ 기간 별로 매크로를 돌려서 쿼리 실행 """

    # string -> datetime object
    start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    end_at = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

    query_count = 0

    interval_num = 1
    kwargs = {interval_type: interval_num}

    while start_time < end_at:

        end_time = start_time + relativedelta(**kwargs)

        print(query_count, ' : ', start_time, ' ~ ', end_time, ' , run at : ', datetime.now().time())

        query_params = {'start_time': start_time, 'end_time': end_time}

        result = connect_postgresql(query, db_name, query_params)  # run query

        save_query_result(save_path, result, query_count)  # save to csv

        # update loop parameters
        start_time = end_time
        query_count += 1

    return None


if __name__ == '__main__':

    print(query)

    db_name = 'BUN_DW'

    try:

        if interval_type:
            run_query_bun_dw_macro(query, save_path, db_name, start_time, end_time, interval_type)

        else:
            run_query_bun_dw_without_macro(query, save_path, db_name)

        print('query completed')
        print('save_path : ', save_path)

    except Exception as e:
        print(e)

    else:
        print('all process was completed')

