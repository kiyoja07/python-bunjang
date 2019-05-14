
from read_service_1 import connect_to_service_1
from datetime import *


def query_macro_days_1(start_time, end_time):

    start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")

    end = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

    query_count = 0

    while True:

        if start_time >= end:
            break

        end_time = start_time + timedelta(days=1)

        query = """
        SELECT uid, keyword, latest_notified, created_at
        FROM search_notification
        WHERE created_at >= %(start_time)s and created_at < %(end_time)s
        """

        query_params = {'start_time': start_time, 'end_time': end_time}

        print(query_count, ' : ', start_time, ' ~ ', end_time)

        # Run SQL
        result = connect_to_service_1(query, query_params)

        if query_count == 1:
            result.to_csv('csv/search_noti_02.csv', index=False, mode='w', header=True)

        else:
            result.to_csv('csv/search_noti_02.csv', index=False, mode='a', header=False)

        start_time = end_time

        query_count += 1


    print('query completed')

    return True


start_time = '2019-02-01 00:00:00'
end_time = '2019-02-28 23:59:59'

query_macro_days_1(start_time, end_time)
