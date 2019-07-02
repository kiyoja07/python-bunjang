import pandas as pd
from connect_db import *
from redshift_query import *
from datetime import *
from dateutil.relativedelta import relativedelta




read_path = '../csv/fraud_reported_since_2018_1.csv'
fraud_reported = pd.read_csv(read_path)

fraud_reported = fraud_reported.dropna()    # Remove missing values

criminal_uid = fraud_reported['criminal_uid']

criminals = criminal_uid.tolist()   # dataframe -> list

criminals = tuple(criminals)    # list -> tuple

print(criminals)

# path to save
save_path = '../csv/test.csv'

# macro parameters
start_time = '2019-01-01 00:00:00'
end_time = '2019-05-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
# query = """
#
# SELECT *
# FROM user_for_stats
# WHERE uid IN %(criminals)s AND join_date >= %(start_time)s AND join_date < %(start_time)s
#
# """

query = """

SELECT *
FROM user_for_stats
WHERE uid in %(criminals)s AND join_date >= %(start_time)s AND join_date < %(start_time)s

"""



def run_query_redshift_without_macro(query, save_path, criminals):

    # result = connect_redshift_without_macro(query)

    query_params = {'criminals': criminals}

    result = connect_redshift(query, query_params)

    result.to_csv(save_path, index=False, mode='w', header=True)

    # print(connect_redshift_without_macro.product_connection_string)

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


        print(query_count, ' : ', start_time, ' ~ ', end_time, ' , run at : ', datetime.now().time())

        result = connect_redshift(query, query_params)

        if query_count == 0:
            result.to_csv(save_path, index=False, mode='w', header=True)

        else:
            result.to_csv(save_path, index=False, mode='a', header=False)

        start_time = end_time

        query_count += 1


    print('query completed')

    return True




def run_query_redshift_macro_everymonth(query, save_path, start_time, end_time, criminals):


    # string -> datetime object
    start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    end = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

    query_count = 0

    while True:

        if start_time >= end:
            break

        end_time = start_time + relativedelta(months=1)

        query_params = {'start_time': start_time, 'end_time': end_time, 'criminals': criminals}

        print(query_count, ' : ', start_time, ' ~ ', end_time, ' , run at :', datetime.now().time())

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

    # a = upload_data()

    print(query)

    # run_query_redshift_without_macro(query, save_path, criminals)

    run_query_redshift_macro_everymonth(query, save_path, start_time, end_time, criminals)


