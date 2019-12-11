



"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/bunp_191101_191130.csv'


# macro parameters
# start_time = '2019-11-01 00:00:00'  # 이상
# end_time = '2019-11-08 00:00:00'  # 미만


interval_type = None
# interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select b.updated_at, b.status as bunp_status, b.seller_pid as pid
from (
select pid
from product_register_history
where updated between '2019-11-01 00:00:00' and '2019-11-07 23:59:59'
) p
join bunjang_promise b
on p.pid = b.seller_pid
where b.updated_at between '2019-11-01 00:00:00' and '2019-11-30 23:59:59'


"""

# ---------------------------------------------------------------------------------

