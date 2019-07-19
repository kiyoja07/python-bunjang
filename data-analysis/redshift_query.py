



"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/user.csv'

# macro parameters
start_time = '2017-01-01 00:00:00'  # 이상
end_time = '2019-07-01 00:00:00'  # 미만

# interval_type = 'months'
interval_type = None

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select uid, join_date, age, sex, bizlicense, device
from user_for_stats
where join_date >= '2017-01-01 00:00:00' AND join_date < '2019-07-01 00:00:00'

"""

# ---------------------------------------------------------------------------------

