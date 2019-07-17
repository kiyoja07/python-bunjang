from connect_db import *
from service_2_query import *
from datetime import *
from dateutil.relativedelta import relativedelta



def save_query_result(save_path, result, query_count):

    if query_count == 0:
        result.to_csv(save_path, index=False, mode='w', header=True)
    else:
        result.to_csv(save_path, index=False, mode='a', header=False)

    return True


def run_query_service_1_without_macro(query, save_path):

    query_count = 0

    print('run at :', datetime.now().time())  # print start time

    # run query
    result = connect_service_2_without_macro(query)

    # save to csv
    save_query_result(save_path, result, query_count)

    print('query completed')  # print end message

    return True




if __name__ == "__main__":

    print(query)

    try:

        run_query_service_1_without_macro(query, save_path)

        print('save_path : ', save_path)

    except ValueError as e:
        print(e)