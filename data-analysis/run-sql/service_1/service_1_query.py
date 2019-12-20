




"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/pid_name_191013_191116.csv'

# macro parameters
# start_time = '2019-10-22 00:00:00.0'  # 이상
# end_time = '2019-11-01 00:00:00.0'  # 미만

interval_type = None
# interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select id as pid, name
from product_info
where create_date between '2019-10-13 00:00:00' and '2019-11-16 23:59:59'


"""


# ---------------------------------------------------------------------------------

