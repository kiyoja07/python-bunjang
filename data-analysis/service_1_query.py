

"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/wanna_buy.csv'


# macro parameters
start_time = '2019-01-01 00:00:00.000000'
end_time = '2019-05-01 00:00:00.000000'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select date_format(i.create_date, '%Y-%m') as month_at,
	c.category, c.name,
	case
		when d.description like '%삽니다%' then 1
		when d.description like '%사요%' then 1
		when d.description like '%구합니다%' then 1
		when d.description like '%구해요%' then 1
	end as wanna_buy,
	count(distinct i.id) as products
from product_info i
join product_description d
on i.id = d.pid
left join categories c
on left(i.category_id, 6) = c.category
where i.create_date >= %(start_time)s AND i.create_date < %(end_time)s
group by 1, 2, 3, 4

"""



