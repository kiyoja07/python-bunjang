



"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/pid_uid_191117_191221.csv'


# macro parameters
# start_time = '2019-11-01 00:00:00'  # 이상
# end_time = '2019-11-08 00:00:00'  # 미만


interval_type = None
# interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select date_trunc('day', p.create_date) as register_date, p.pid, p.uid, p.device, u.bizlicense
from product_info_for_stats p
join user_for_stats u
on p.uid = u.uid
where p.create_date between '2019-11-17 00:00:00' and '2019-12-21 23:59:59'


"""

# ---------------------------------------------------------------------------------

