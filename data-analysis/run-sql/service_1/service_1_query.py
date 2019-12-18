




"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/reward_register_2.0_all_user.csv'

# macro parameters
# start_time = '2019-10-22 00:00:00.0'  # 이상
# end_time = '2019-11-01 00:00:00.0'  # 미만

interval_type = None
# interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select date_format(u.join_date, '%Y-%m-%d') as join_date, u.id as uid
from user u
where date_format(u.join_date, '%Y-%m-%d') in ('2019-12-04', '2019-12-05', '2019-12-08')


"""


# ---------------------------------------------------------------------------------
# where u.update_time between '2019-10-22 00:00:00' and '2019-10-31 23:59:59' # UP 실행 기간
