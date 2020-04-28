



"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/item_click_all.csv'


# macro parameters
# start_time = '2019-11-01 00:00:00'  # 이상
# end_time = '2019-11-08 00:00:00'  # 미만


interval_type = None
# interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select p.category_id, count(c.id) as click_count
from item_click_log6 c
join product_info_for_stats p
on c.target_id = p.pid
where c.keyword not in ('', 'undefined') and c.updated between '2019-02-01 00:00:00' and '2020-01-31 23:59:59'
group by 1


"""

# ---------------------------------------------------------------------------------

