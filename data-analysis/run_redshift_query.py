from connect_db import *
from redshift_query import *
from datetime import *
from dateutil.relativedelta import relativedelta


def run_query_redshift_without_macro(query, save_path):

    print('run at :', datetime.now().time())  # print start time

    result = connect_redshift_without_macro(query)  # run query

    result.to_csv(save_path, index=False, mode='w', header=True)  # save to csv

    print('query completed')  # print end message

    return True



def run_query_redshift_macro(query, save_path, start_time, end_time, interval_type):

    start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")

    end = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

    query_count = 0

    while True:

        if start_time >= end:
            break


        interval_num = 1

        kwargs = {interval_type: interval_num}

        end_time = start_time + relativedelta(**kwargs)

        query_params = {'start_time': start_time, 'end_time': end_time}

        print(query_count, ' : ', start_time, ' ~ ', end_time, ' , run at : ', datetime.now().time())

        # run query
        result = connect_redshift(query, query_params)

        # save to csv
        if query_count == 0:
            result.to_csv(save_path, index=False, mode='w', header=True)
        else:
            result.to_csv(save_path, index=False, mode='a', header=False)

        start_time = end_time

        query_count += 1


    print('query completed')

    return True



if __name__ == "__main__":

    print(query)

    try:

        if interval_type is None:
            run_query_redshift_without_macro(query, save_path)

        else:
            run_query_redshift_macro(query, save_path, start_time, end_time, interval_type)

        print('save_path : ', save_path)

    except ValueError as e:
        print(e)



