



"""
Query & Save Path
"""

# path to save
save_path = 'csv/bunp_category_since_1801.csv'


# macro parameters
# start_time = '2019-04-01 00:00:00'
# end_time = '2019-05-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """


-- 카테고리별 번프 완료된 상품 수

-- 전체 구매 데이터
WITH bunp_all AS (
SELECT seller_pid, date_trunc('day', updated_at) AS updated_at, seller_pid_price AS bunp_amount
FROM bunjang_promise
WHERE status = 4 AND seller_pid_price > 0 AND seller_pid_price < 3000000 AND 
    updated_at >= '2018-01-01' AND updated_at < '2019-05-01'
),
-- 구매 상품의 카테고리 id, 18년 1월 1일 이후 등록된 상품만
bunp_cate AS (
SELECT category_id AS cate_id, a.updated_at, a.seller_pid, a.bunp_amount
FROM bunp_all a
JOIN product_info_for_stats p
ON a.seller_pid = p.pid
where create_date >= '2018-01-01 00:00:00'
)


--ㅡ구매 상품의 카테고리 이름
SELECT c.category, c.name, b.updated_at, b.seller_pid, b.bunp_amount
FROM bunp_cate b
LEFT JOIN categories c
ON b.cate_id = c.category



"""