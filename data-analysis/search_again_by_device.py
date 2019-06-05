from connect_db import connect_redshift_direct
import csv


def run_query(query):


    result = connect_redshift_direct(query)

    result.to_csv('csv/search_again_1904.csv', index=False, mode='w', header=True)

    print('Done')

    return True



if __name__ == "__main__":

    query = """
    SELECT _user.uid, _user.device, _search.keyword, COUNT(_search.id) as count_search
    FROM item_search_log _search
    JOIN user_for_stats _user
    ON _search.viewer_uid = _user.uid
    WHERE _search.updated >= '2019-04-01 00:00:00' AND _search.updated >= '2019-04-30 23:59:59'
    GROUP BY 1, 2, 3
     """

    run_query(query)

import json
from pandas.io.json import json_normalize


def json_parsing(row):
    try:

        dic = json.loads(row)

        buyer_uid = dic['buyer_uid']

        return buyer_uid

    except:
        print('this row is error')


df_bunp_test['buyer_uid'] = df_bunp['params'].apply(json_parsing)
df_bunp_test