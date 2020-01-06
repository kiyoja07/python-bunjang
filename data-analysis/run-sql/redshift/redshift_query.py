



"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/reward_pay_1.0_bunp.csv'


# macro parameters
# start_time = '2019-11-01 00:00:00'  # 이상
# end_time = '2019-11-08 00:00:00'  # 미만


interval_type = None
# interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select channel_id, seller_uid, buyer_uid, seller_pid, created_at
from bunjang_promise
where created_at >= '2019-12-18 00:00:00' AND created_at < '2020-01-04 00:00:00'


"""

# ---------------------------------------------------------------------------------

