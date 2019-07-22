



"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/register.csv'

# macro parameters
start_time = '2018-01-01 00:00:00'  # 이상
end_time = '2019-07-01 00:00:00'  # 미만

interval_type = 'months'
# interval_type = None

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select date_trunc('month', p.create_date) as month, c.name, count(distinct p.pid)
from product_info_for_stats p
join categories c
on left(p.category_id, 3) = c.category
group by 1, 2
order by 1

"""

# ---------------------------------------------------------------------------------

