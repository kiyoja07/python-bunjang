




"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/sold_product.csv'

# macro parameters
# start_time = '2019-01-01 00:00:00'
# end_time = '2019-03-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

-- 월별 예약, 삭제, 판매 완료된 제품
-- status IN (1, 2, 3)
SELECT distinct date_trunc('month', updated) AS sold_at, pid
FROM product_status_change_log
WHERE status IN (1, 2, 3)



"""

# ---------------------------------------------------------------------------------
