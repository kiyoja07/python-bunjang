
from read_redshift import connect_redshift
from datetime import *


def query_macro_hours_1(start_time, end_time):

    start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")

    end = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

    query_count = 0

    while True:

        if start_time >= end:
            break

        end_time = start_time + timedelta(hours=1)

        query = """
        SELECT updated, viewer_uid, ref_page, keyword
        FROM item_click_log6
        WHERE updated >= %(start_time)s and updated < %(end_time)s
        """

        query_params = {'start_time': start_time, 'end_time': end_time}

        print(query_count, ' : ', start_time, ' ~ ', end_time)

        # Run SQL
        result = connect_redshift(query, query_params)

        if query_count == 1:
            result.to_csv('csv/item_click_0301.csv', index=False, mode='w', header=True)

        else:
            result.to_csv('csv/item_click_0301.csv', index=False, mode='a', header=False)

        start_time = end_time

        query_count += 1


    print('query completed')

    return True


start_time = '2019-03-01 00:00:00'
end_time = '2019-03-01 23:59:59'

query_macro_hours_1(start_time, end_time)
