import pandas as pd
import numpy as np


noti_path = 'csv/search_noti_02.csv'
search_path = 'csv/item_search_02.csv'
# click_path = 'csv/item_click_0301.csv'

# noti_df = pd.read_csv(noti_path).dropna(axis=0)
search_df = pd.read_csv(search_path).dropna(axis=0)
# click_df = pd.read_csv(click_path).dropna(axis=0)

# noti_df = noti_df[noti_df.uid > 0]
search_df = search_df[search_df.viewer_uid > 0]


def more(row):
    latest_notified = row['latest_notified']
    created_at = row['created_at']

    if latest_notified >= created_at:
        more = 1
    else:
        more = 0
    return more


noti_df_tmp = noti_df
noti_df_tmp.loc[:, 'tmp'] = noti_df[['latest_notified', 'created_at']].apply(more, axis=1)
print(noti_df_tmp)