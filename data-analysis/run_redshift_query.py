from connect_db import connect_redshift
from redshift_query import *
from datetime import *
from dateutil.relativedelta import relativedelta


def save_query_result(save_path, result, query_count):
    """ 쿼리 결과를 csv로 저장 """

    if query_count == 0:
        result.to_csv(save_path, index=False, mode='w', header=True)
    else:
        result.to_csv(save_path, index=False, mode='a', header=False)

    return None


def run_query_redshift_without_macro(query, save_path):
    """ 매크로 없이 쿼리 실행 """

    query_count = 0
    query_params = None

    print('run at :', datetime.now().time())  # print start time

    # run query
    result = connect_redshift(query, query_params)
    # result = connect_redshift_without_macro(query)

    # save to csv
    save_query_result(save_path, result, query_count)

    print('query completed')  # print end message

    return None



def run_query_redshift_macro(query, save_path, start_time, end_time, interval_type):
    """ 기간 별로 매크로를 돌려서 쿼리 실행 """

    start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    end_at = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

    query_count = 0

    interval_num = 1
    kwargs = {interval_type: interval_num}

    while start_time < end_at:

        end_time = start_time + relativedelta(**kwargs)

        print(query_count, ' : ', start_time, ' ~ ', end_time, ' , run at : ', datetime.now().time())

        # run query
        query_params = {'start_time': start_time, 'end_time': end_time}
        result = connect_redshift(query, query_params)

        # save to csv
        save_query_result(save_path, result, query_count)

        # update loop parameters
        start_time = end_time
        query_count += 1


    print('query completed')

    return None



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



