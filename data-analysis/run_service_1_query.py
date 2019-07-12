from connect_db import *
from service_1_query import *
from datetime import *
from dateutil.relativedelta import relativedelta


def run_query_service_1_without_macro(query, save_path):

    print('run at :', datetime.now().time())

    result = connect_service_1_without_macro(query)  # run query

    result.to_csv(save_path, index=False, mode='w', header=True)  # save as csv

    print('query completed')

    return True



def run_query_service_1_macro_everymonth(query, save_path, start_time, end_time):

    # string -> datetime object
    start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S.%f")
    end = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S.%f")

    query_count = 0

    while True:

        if start_time >= end:
            break

        end_time = start_time + relativedelta(months=1)

        query_params = {'start_time': start_time, 'end_time': end_time}

        print(query_count, ' : ', start_time, ' ~ ', end_time, ' , run at :', datetime.now().time())

        # run query
        result = connect_service_1(query, query_params)

        # save as csv
        if query_count == 0:
            result.to_csv(save_path, index=False, mode='w', header=True)

        else:
            result.to_csv(save_path, index=False, mode='a', header=False)

        # update loop parameters
        start_time = end_time

        query_count += 1

    print('query completed')

    return True



if __name__ == "__main__":

    print(query)

    # run_query_service_1_without_macro(query, save_path)

    run_query_service_1_macro_everymonth(query, save_path, start_time, end_time)

    print('save_path : ', save_path)

    # try:
    #     run_query_service_1_without_macro(query, save_path)
    #
    # except:
    #
    #     try:
    #         # run_query_redshift_macro_everyday(query, save_path, start_time, end_time)
    #         run_query_service_1_macro_everymonth(query, save_path, start_time, end_time)
    #
    #     except:
    #         print('FAIL, query could not run')
