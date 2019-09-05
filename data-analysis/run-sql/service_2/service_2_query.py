
"""
DB : Service 2, MySQL
Query & Save Path
"""


# path to save
save_path = '../../csv/test_1.csv'

# interval_type = 'months'
interval_type = None

# query to run
query = \
"""

-- 미발송으로 상담센터 신고건의 신고자 및 대상자 uid
select d.discussion_id, d.created_at, d.reporter_uid, i.value as fraud_uid
from help_extra_info i
join (
    select discussion_id, created_at, created_by as reporter_uid
    from help_discussion
    where category_id = 'transaction-no-ship'
) d
on i.discussion_id = d.discussion_id
where i.key_name = 'targetUid'
limit 10

"""
# ---------------------------------------------------------------------------------
