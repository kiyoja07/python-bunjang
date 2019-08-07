



"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/product_registered.csv'

# macro parameters
# start_time = '2018-01-01 00:00:00'  # 이상
# end_time = '2019-07-01 00:00:00'  # 미만

# interval_type = 'months'
interval_type = None

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select updated, pid
from product_register_history


"""

# ---------------------------------------------------------------------------------

