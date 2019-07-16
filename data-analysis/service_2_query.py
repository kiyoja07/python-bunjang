
"""
DB : Service 2, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/help_reporter_frauder.csv'


# query to run
query = \
"""

-- 거래관련 상담센터 신고건의 신고자 및 대상자 uid
select d.discussion_id, d.created_at, d.reporter_uid, i.value as fraud_uid
from help_extra_info i
join (
    select discussion_id, created_at, created_by as reporter_uid
    from help_discussion
    where category_id in 
        ('transaction',
        'transaction-bad-item',
        'transaction-bunpay',
        'transaction-buyer',
        'transaction-etc',
        'transaction-no-paid',
        'transaction-no-ship',
        'transaction-no-ship-fee',
        'transaction-refund',
        'transaction-seller',
        'transaction-trade',
        'etc-join')
) d
on i.discussion_id = d.discussion_id
where i.key_name = 'targetUid'

"""
# ---------------------------------------------------------------------------------
