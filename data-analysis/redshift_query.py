


"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/registered_product.csv'

# macro parameters
# start_time = '2019-01-01 00:00:00'
# end_time = '2019-03-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

-- 최초 등록
-- 월별 등록된 상품
SELECT date_trunc('day', updated) AS registered_at, pid
FROM product_register_history


"""

# ---------------------------------------------------------------------------------


