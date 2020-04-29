import os
import sys
sys.path.append(os.path.dirname(os.path.abspath(os.path.dirname(__file__))))

from connect_db import connect_redshift
from redshift_query import *
from datetime import datetime
from dateutil.relativedelta import relativedelta
from save_query_result import save_query_result


def run_query_redshift(query, save_path, db_name, start_time=None, end_time=None, interval_type=None, interval_num=1):

    if start_time and end_time and interval_type:

        # string -> datetime object
        start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
        end_at = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

        query_count = 0

        kwargs = {interval_type: interval_num}

        while start_time < end_at:
            end_time = start_time + relativedelta(**kwargs)

            print(query_count, ' : ', start_time, ' ~ ', end_time, ' , run at : ', datetime.now().time())

            query_params = {'start_time': start_time, 'end_time': end_time}

            result = connect_redshift(query, db_name, query_params)  # run query

            save_query_result(save_path, result, query_count)  # save to csv

            # update loop parameters
            start_time = end_time
            query_count += 1

    else:

        print('run at :', datetime.now().time())  # print start time

        result = connect_redshift(query, db_name)  # run query

        save_query_result(save_path, result)  # save to csv

    return None


if __name__ == '__main__':

    print(query)

    db_name = 'REDSHIFT'

    try:
        if interval_type:
            run_query_redshift(query, save_path, db_name, start_time, end_time, interval_type)
        else:
            run_query_redshift(query, save_path, db_name)

        print('query completed', f'save_path : {save_path}', sep='\n')

    except Exception as e:
        print(e)

    else:
        print('all process was completed')



