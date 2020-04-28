

"""
DB : BUN_DW, PostgreSQL
Query & Save Path
"""


# path to save
save_path = '../../csv/test_bun_dw.csv'


# macro parameters
# start_time = '2019-11-01 00:00:00'  # 이상
# end_time = '2019-11-08 00:00:00'  # 미만


interval_type = None
# interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select *
from bun_log_db.app_event_type_search
where year||month||day = '20200412'
limit 10

"""

# ---------------------------------------------------------------------------------

