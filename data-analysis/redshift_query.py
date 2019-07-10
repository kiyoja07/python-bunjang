


"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/bunp_history_all.csv'

# macro parameters
# start_time = '2019-01-01 00:00:00'
# end_time = '2019-03-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select b.created_at, c.category, b.status, b.buyer_uid, b.seller_uid, b.seller_pid_price
from bunjang_promise b
left join product_info_for_stats p
on b.seller_pid = p.pid
left join categories c
on p.category_id = c.category
where b.seller_pid_price > 0 and b.created_at < '2019-07-01'

"""
