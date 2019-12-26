




"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/ad_point_191013_191221.csv'

# macro parameters
# start_time = '2019-10-22 00:00:00.0'  # 이상
# end_time = '2019-11-01 00:00:00.0'  # 미만

interval_type = None
# interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select date_format(updated,'%Y-%m-%d') as date, uid, sum(price) as price
from sponsor_transaction_log
where status = 0 and updated between '2019-10-13 00:00:00' and '2019-12-21 23:59:59'
group by 1, 2


"""


# ---------------------------------------------------------------------------------

