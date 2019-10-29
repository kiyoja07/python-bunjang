
"""
DB : Quicket_Log, MySQL
Query & Save Path
"""


# path to save
save_path = '../../csv/test_quicket_log.csv'

# macro parameters
start_time = '2019-01-01 00:00:00.0'  # 이상
end_time = '2019-01-03 00:00:00.0'  # 미만

interval_type = 'days'
# interval_type = None


# query to run
query = \
"""

-- Daily 번개톡 최초 메시지 발송 수
select date_format(update_time, '%%Y-%%m-%%d'), count(*)
from tb_message
where message_no = 1 and
	update_time >= %(start_time)s AND update_time < %(end_time)s
group by 1
"""
# ---------------------------------------------------------------------------------
