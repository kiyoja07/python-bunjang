
from connect_db import connect_redshift
import pandas as pd
from datetime import *
import csv


# 거래 건의 시간, seller, buyer 리스트
def query_macro_hours_1(start_time, end_time):

    start_time = datetime.strptime(start_time, "%Y-%m-%d %H:%M:%S")

    end = datetime.strptime(end_time, "%Y-%m-%d %H:%M:%S")

    query_count = 0

    while True:

        if start_time >= end:
            break

        end_time = start_time + timedelta(hours=1)

        query = """
        SELECT updated, 
            json_extract_path_text(params, 'buyer_uid') AS buyer_uid,
            json_extract_path_text(params, 'seller_pid') AS seller_pid
        FROM bunp
        WHERE updated >= %(start_time)s and updated < %(end_time)s AND 
            log_type = 'make'
        """

        query_params = {'start_time': start_time, 'end_time': end_time}

        print(query_count, ' : ', start_time, ' ~ ', end_time)

        # Run SQL
        result = connect_redshift(query, query_params)

        if query_count == 1:
            result.to_csv('csv/bunp_0430.csv', index=False, mode='w', header=True)

        else:
            result.to_csv('csv/bunp_0430.csv', index=False, mode='a', header=False)

        start_time = end_time

        query_count += 1


    print('query completed')

    return True


start_time = '2019-04-30 00:00:00'
end_time = '2019-04-30 23:59:59'

# query_macro_hours_1(start_time, end_time)





def query_macro_click(buyer, product):

    query = """
    SELECT updated, viewer_uid, target_id, ref_page, keyword
    FROM item_click_log6
    WHERE viewer_uid = %(buyer)s AND target_id = %(product)s 
    """

    query_params = {'buyer': buyer, 'product': product}

    # Run SQL
    result = connect_redshift(query, query_params)

    return result




with open('csv/bunp_0430.csv') as csvfile:

    csv_reader = csv.reader(csvfile, delimiter=',')

    next(csv_reader, None)

    for i, row in enumerate(csv_reader):

        result = query_macro_click(buyer=row[1], product=row[2])

        result = pd.DataFrame(result)

        if i == 0:
            result.to_csv('csv/click_bunp.csv', index=False, mode='w', header=True)

        else:
            result.to_csv('csv/click_bunp.csv', index=False, mode='a', header=False)

        print(i)

    print('Done')



