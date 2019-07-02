
"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/test.csv'

# macro parameters
start_time = '2019-01-01 00:00:00'
end_time = '2019-03-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

SELECT *
FROM user_for_stats
WHERE uid IN ('12324', '324234') AND join_date = %(start_time)s
"""
