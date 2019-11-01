
"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/reward_test_bunp.csv'

# macro parameters
start_time = '2019-10-22 00:00:00.0'  # 이상
end_time = '2019-11-01 00:00:00.0'  # 미만

# interval_type = None
interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select channel_id, seller_uid, buyer_uid, seller_pid, created_at
from bunjang_promise
where created_at >= %(start_time)s AND created_at < %(end_time)s


"""

# ---------------------------------------------------------------------------------
