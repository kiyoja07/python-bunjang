from connect_db import *
from query import *
from datetime import *


def run_query_redshift_without_macro(query, save_path):


    result = connect_redshift_without_macro(query)

    result.to_csv(save_path, index=False, mode='w', header=True)

    print('query completed')

    return True



def run_query_redshift_macro_everyday(query, save_path, start_time, end_time):

    start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")

    end = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

    query_count = 0

    while True:

        if start_time >= end:
            break

        end_time = start_time + timedelta(days=1)

        query_params = {'start_time': start_time, 'end_time': end_time}

        print(query_count, ' : ', start_time, ' ~ ', end_time)

        result = connect_redshift(query, query_params)

        if query_count == 0:
            result.to_csv(save_path, index=False, mode='w', header=True)

        else:
            result.to_csv(save_path, index=False, mode='a', header=False)

        start_time = end_time

        query_count += 1


    print('query completed')

    return True



if __name__ == "__main__":

    try:
        run_query_redshift_without_macro(query, save_path)

    except:

        try:
            run_query_redshift_macro_everyday(query, save_path, start_time, end_time)

        except:
            print('FAIL, query could not run')



