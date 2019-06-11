




"""
Query & Save Path
"""

# path to save
save_path = 'csv/clicked.csv'


# macro parameters
start_time = '2018-01-01 00:00:00'
end_time = '2019-05-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

-- 19년 4월 클릭된 상품의 검색 키워드와 세부 카테고리


SELECT tmp.date_at, tmp.category_id, c.name, tmp.clicked
FROM (
    SELECT date_trunc('day', i.updated) AS date_at, p.category_id, COUNT(i.id) AS clicked
    FROM item_click_log6 i
    LEFT JOIN product_info_for_stats p
    ON i.target_id = p.pid
    WHERE i.updated >= %(start_time)s AND i.updated < %(end_time)s
    GROUP BY 1, 2
) tmp
LEFT JOIN categories c
ON tmp.category_id = c.category


"""