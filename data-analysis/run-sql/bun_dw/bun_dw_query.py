

"""
DB : BUN_DW, PostgreSQL
Query & Save Path
"""


# path to save
save_path = '../../csv/search_click_200501_200510.csv'


# macro parameters
# start_time = '2019-11-01 00:00:00'  # 이상
# end_time = '2019-11-08 00:00:00'  # 미만


interval_type = None
# interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select v.ref_term, c.category, c.name, count(v.content_id) as click
from bun_log_db.app_event_type_view v
join service1_quicket.product_info p
on v.content_id = p.id
join service1_quicket.categories c
on p.category_id = c.category
where year||month||day between '20200501' and '20200510'
    and v.ref_term is not null
group by 1, 2, 3
order by click desc

"""

# ---------------------------------------------------------------------------------

