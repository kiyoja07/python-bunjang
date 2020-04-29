

"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = '../../csv/test_redshift_1.csv'


# macro parameters
# start_time = '2019-11-01 00:00:00'  # 이상
# end_time = '2019-11-04 00:00:00'  # 미만


interval_type = None
# interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select updated
from item_click_log6
where updated >= '2019-11-01 00:00:00' and updated < '2019-11-04 00:00:00'
limit 10


"""

# ---------------------------------------------------------------------------------



#
# """
# DB : Redshift, PostgreSQL
# Query & Save Path
# """
#
#
# # path to save
# save_path = '../../csv/test_redshift.csv'
#
#
# # macro parameters
# start_time = '2019-11-01 00:00:00'  # 이상
# end_time = '2019-11-04 00:00:00'  # 미만
#
#
# # interval_type = None
# interval_type = 'days'
# # interval_type = 'months'
#
#
# # updated >= %(start_time)s AND updated < %(end_time)s
#
# # query to run
# query = """
#
# select updated
# from item_click_log6
# where updated >= %(start_time)s and updated < %(end_time)s
# limit 10
#
#
# """
#
# # ---------------------------------------------------------------------------------
