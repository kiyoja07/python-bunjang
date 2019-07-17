




"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/test.csv'

# macro parameters
start_time = '2019-01-01 00:00:00'
end_time = '2019-04-01 00:00:00'

interval_type = 'months'
# interval_type = None

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select id
from user_join_log
where updated >= %(start_time)s AND updated < %(end_time)s


"""

# ---------------------------------------------------------------------------------
