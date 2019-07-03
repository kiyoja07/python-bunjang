
"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/bunp_history.csv'

# macro parameters
# start_time = '2019-01-01 00:00:00'
# end_time = '2019-03-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

SELECT updated_at, buyer_uid, seller_uid, seller_pid_price
FROM bunjang_promise
WHERE status = 4 AND seller_pid_price > 0

"""
