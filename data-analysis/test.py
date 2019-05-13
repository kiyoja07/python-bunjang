from read_redshift import connect_redshift
from datetime import *
# import time

def query_macro_date(start_time, end):

    start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")
    start = start_time.time()

    end = datetime.strptime(end, "%Y-%m-%d %H:%M:%S").time()

    while True:

        start = start_time

        if start.time() >= end:
            break

        print(start, end)
        end_time= start_time+ timedelta(hours=1)

        query = """
        select updated, json_data
        from pageview
        where updated >= %(start)s and updated < %(end)s
        """

        query_params = {'start': start_time, 'end': end_time}

        result = connect_redshift(query, query_params)

        print(result)

        start_time = end_time

    return True



start_time = '2019-05-01 00:00:00'
end = '2019-05-01 03:00:00'
query_macro_date(start_time, end)

# print(result)

# start_time = '2019-05-01 00:00:00'
# end_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S") + timedelta(hours=1)
#
# print(end_time)


# query = """
# select updated, json_data
# from pageview
# where updated >= %(start)s and updated < %(start)s::timestamp + interval '1 hour'
# """

# query_params = {'start': start_time}
#
# print(connect_redshift(query, query_params))
