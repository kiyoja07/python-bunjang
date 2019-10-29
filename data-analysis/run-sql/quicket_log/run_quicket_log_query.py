import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from connect_db import connect_main_db
from quicket_log_query import *
from datetime import *
from dateutil.relativedelta import relativedelta
from save_query_result import save_query_result


def run_query_quicket_log_without_macro(query, save_path):
    """ 매크로 없이 쿼리 실행 """

    query_count = 0
    query_params = None

    print('run at :', datetime.now().time())

    result = connect_main_db(query, query_params, db_name)  # run query

    save_query_result(save_path, result, query_count)  # save to csv

    print('query completed')

    return None


def run_query_quicket_log_macro(query, save_path, start_time, end_time, interval_type, db_name):
    """ 기간 별로 매크로를 돌려서 쿼리 실행 """

    # string -> datetime object
    start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S.%f")
    end_at = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S.%f")

    query_count = 0

    interval_num = 1
    kwargs = {interval_type: interval_num}

    while start_time < end_at:

        end_time = start_time + relativedelta(**kwargs)

        print(query_count, ' : ', start_time, ' ~ ', end_time, ' , run at :', datetime.now().time())

        query_params = {'start_time': start_time, 'end_time': end_time}

        result = connect_main_db(query, query_params, db_name)  # run query

        save_query_result(save_path, result, query_count)  # save to csv

        # update loop parameters
        start_time = end_time
        query_count += 1

    print('query completed')

    return None


if __name__ == "__main__":

    print(query)

    db_name = 'QUICKET_LOG'

    try:

        if interval_type is None:
            run_query_quicket_log_without_macro(query, save_path)

        else:
            run_query_quicket_log_macro(query, save_path, start_time, end_time, interval_type, db_name)

        print('save_path : ', save_path)

    except Exception as e:
        print(e)

    else:
        print('all process was completed')
