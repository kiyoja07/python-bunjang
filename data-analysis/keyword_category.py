from connect_db import connect_redshift
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
        SELECT tmp.keyword, c.category, c.name, COUNT(*) AS _count
        FROM (
            SELECT i.keyword, left(p.category_id, 3) AS category_id
            FROM item_click_log6 i
            LEFT JOIN product_info_for_stats p
            ON i.target_id = p.pid
            WHERE i.updated >= %(start_time)s AND i.updated < %(end_time)s AND
                i.keyword <> '' AND p.category_id <> ''
        ) tmp
        LEFT JOIN categories c
        ON tmp.category_id = c.category
        GROUP BY 1, 2, 3
        """

        query_params = {'start_time': start_time, 'end_time': end_time}

        print(query_count, ' : ', start_time, ' ~ ', end_time)

        result = connect_redshift(query, query_params)

        save_path = 'csv/keyword_category.csv'

        if query_count == 0:
            result.to_csv(save_path, index=False, mode='w', header=True)

        else:
            result.to_csv(save_path, index=False, mode='a', header=False)

        start_time = end_time

        query_count += 1


    print('query completed')

    return True


start_time = '2019-04-01 00:00:00'
end_time = '2019-05-01 00:00:00'

query_macro_days_1(start_time, end_time)
