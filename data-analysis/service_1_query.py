
"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/test.csv'


# macro parameters
start_time = '2019-01-01 00:00:00.000000'
end_time = '2019-04-01 00:00:00.000000'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = \
"""

select *
from user
where join_date >= %(start_time)s AND join_date < %(end_time)s

"""

# ---------------------------------------------------------------------------------