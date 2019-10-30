
"""
DB : Quicket_Log, MySQL
Query & Save Path
"""


# path to save
save_path = '../../csv/chat_test.csv'

# macro parameters
start_time = '2019-10-22 00:00:00.0'  # 이상
end_time = '2019-11-01 00:00:00.0'  # 미만

interval_type = 'days'
# interval_type = None


# query to run
query = \
"""

select distinct extras, channel_id
from tb_message
where typecode= 12 and
    update_time >= %(start_time)s AND update_time < %(end_time)s
"""



# ---------------------------------------------------------------------------------
