
"""
Query Archive
"""


"""
DB : Service 2, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/help_uid.csv'


# query to run
query = """

-- 거래관련 신고건의 신고 대상 uid
select distinct i.value
from help_extra_info i
join (
    select discussion_id
    from help_discussion
    where status = 1 and category_id in 
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





