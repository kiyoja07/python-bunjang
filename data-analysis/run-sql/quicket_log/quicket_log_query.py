
"""
DB : Quicket_Log, MySQL
Query & Save Path
"""


# path to save
save_path = '../../csv/reward_test_chat_1107_1120.csv'

# macro parameters
start_time = '2019-11-07 00:00:00.0'  # 이상
end_time = '2019-11-21 00:00:00.0'  # 미만

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
