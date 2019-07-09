


"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/pay_seller_buyer.csv'

# macro parameters
# start_time = '2019-01-01 00:00:00'
# end_time = '2019-03-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select date_trunc('day', m.create_date) as date, m.order_status_cd, m.buyer_id, o.seller_id, m.total_price, o.category_id
from (
	select id, create_date, order_status_cd, buyer_id, total_price
	from order_mast
	where create_date < '2019-07-01'
) m
join (
	select i.order_mast_id, i.seller_id, p.category_id
	from order_item i
	left join product_info_for_stats p
	on i.pid = p.pid
) o
on m.id = o.order_mast_id

"""
