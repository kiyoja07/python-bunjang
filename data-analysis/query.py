



"""
Query & Save Path
"""

# path to save
save_path = 'csv/product_register.csv'


# macro parameters
# start_time = '2019-04-01 00:00:00'
# end_time = '2019-05-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """


SELECT date_trunc('month', register_date) AS month, category_id, status, count(DISTINCT id)
FROM product_info_for_stats
GROUP BY 1, 2, 3
where register_date >= '2017-01-01 00:00:00' and register_date <= '2019-05-01 00:00:00'


"""



SELECT *
FROM product_status_change_log
WHERE pid = '94973272'
ORDER BY updated




SELECT *
FROM product_info_for_stats
WHERE create_date >= '2019-01-01'
LIMIT 5000



SELECT *
FROM product_info_for_stats
WHERE pid = '94973272'
LIMIT 5000
