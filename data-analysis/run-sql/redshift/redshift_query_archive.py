
"""
Query Archive
"""


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


"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/item_click_400.csv'


# macro parameters
# start_time = '2019-11-01 00:00:00'  # 이상
# end_time = '2019-11-08 00:00:00'  # 미만


interval_type = None
# interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select p.category_id, t.name, c.keyword, count(c.id) as click_count
from item_click_log6 c
join product_info_for_stats p
on c.target_id = p.pid
join categories t
on p.category_id = t.category
where c.keyword <> '' and c.updated between '2019-02-01 00:00:00' and '2020-01-31 23:59:59' and
	left(p.category_id, 3) in ('400')
group by 1, 2, 3


"""

# ---------------------------------------------------------------------------------




"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/bunp_191101_191130.csv'


# macro parameters
# start_time = '2019-11-01 00:00:00'  # 이상
# end_time = '2019-11-08 00:00:00'  # 미만


interval_type = None
# interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select b.updated_at, b.status, b.seller_pid as pid
from (
select pid
from product_register_history
where updated between '2019-11-01 00:00:00' and '2019-11-07 23:59:59'
) p
join bunjang_promise b
on p.pid = b.seller_pid
where b.updated_at between '2019-11-01 00:00:00' and '2019-11-30 23:59:59'


"""




"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/status_191101_191130.csv'


# macro parameters
# start_time = '2019-11-01 00:00:00'  # 이상
# end_time = '2019-11-08 00:00:00'  # 미만


interval_type = None
# interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select s.updated, s.status, s.pid
from (
select pid
from product_register_history
where updated between '2019-11-01 00:00:00' and '2019-11-07 23:59:59'
) p
join product_status_change_log s
on p.pid = s.pid
where s.updated between '2019-11-01 00:00:00' and '2019-11-30 23:59:59'


"""


"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/register_191101_191107.csv'


# macro parameters
# start_time = '2019-11-01 00:00:00'  # 이상
# end_time = '2019-11-08 00:00:00'  # 미만


interval_type = None
# interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select updated, pid
from product_register_history
where updated between '2019-11-01 00:00:00' and '2019-11-07 23:59:59'


"""



"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/click_1911.csv'


# macro parameters
start_time = '2019-11-01 00:00:00'  # 이상
end_time = '2019-11-08 00:00:00'  # 미만


# interval_type = None
interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select updated, target_id, keyword
from item_click_log6
where updated >= %(start_time)s and updated < %(end_time)s


"""




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



"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/user.csv'

# macro parameters
start_time = '2017-01-01 00:00:00'  # 이상
end_time = '2019-07-01 00:00:00'  # 미만

# interval_type = 'months'
interval_type = None

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select uid, join_date, age, sex, bizlicense, device
from user_for_stats
where join_date >= '2017-01-01 00:00:00' AND join_date < '2019-07-01 00:00:00'

"""

# ---------------------------------------------------------------------------------



"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/product.csv'

# macro parameters
start_time = '2017-01-01 00:00:00'  # 이상
end_time = '2019-07-01 00:00:00'  # 미만

interval_type = 'months'
# interval_type = None

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select pid, uid, create_date, category_id, price
from product_info_for_stats
where create_date >= %(start_time)s AND create_date < %(end_time)s

"""

# ---------------------------------------------------------------------------------





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



"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/re_registered_product.csv'

# macro parameters
# start_time = '2019-01-01 00:00:00'
# end_time = '2019-03-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

-- 월별 다시 판매 중으로 바꾼 상품
-- status가 (1, 2, 3) -> 0
SELECT distinct date_trunc('month', updated) AS re_registered_at, p.pid
FROM product_status_change_log p
JOIN (
SELECT DATE_trunc('month', updated) AS sold_at, pid
FROM product_status_change_log
WHERE status IN (1, 2, 3)
) s
ON p.pid = s.pid AND DATE_trunc('month', p.updated)> s.sold_at AND p.status = 0


"""

# ---------------------------------------------------------------------------------



"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/re_sold_product.csv'

# macro parameters
# start_time = '2019-01-01 00:00:00'
# end_time = '2019-03-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

-- 판매 불가 status에서 다시 판매 불가 status로 바뀐 제품
-- status가 (1, 2, 3) -> (1, 2, 3)
SELECT distinct date_trunc('month', updated) AS re_sold_at, p.pid
FROM product_status_change_log p
JOIN (
	SELECT DATE_trunc('month', updated) AS sold_at, pid
	FROM product_status_change_log
	WHERE status IN (1, 2, 3)
) s
ON p.pid = s.pid AND DATE_trunc('month', p.updated)> s.sold_at AND p.status IN (1, 2, 3)


"""

# ---------------------------------------------------------------------------------


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
SELECT distinct date_trunc('month', updated) AS registered_at, pid
FROM product_register_history


"""

# ---------------------------------------------------------------------------------





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

# ---------------------------------------------------------------------------------



"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/bunp_history_all.csv'

# macro parameters
# start_time = '2019-01-01 00:00:00'
# end_time = '2019-03-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select b.created_at, c.category, b.status, b.buyer_uid, b.seller_uid, b.seller_pid_price
from bunjang_promise b
left join product_info_for_stats p
on b.seller_pid = p.pid
left join categories c
on p.category_id = c.category
where b.seller_pid_price > 0 and b.created_at < '2019-07-01'

"""

# ---------------------------------------------------------------------------------




"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/categories.csv'

# macro parameters
# start_time = '2019-01-01 00:00:00'
# end_time = '2019-03-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select *
from categories

"""




"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/bunp_history.csv'

# macro parameters
# start_time = '2019-01-01 00:00:00'
# end_time = '2019-03-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select b.updated_at, c.category, b.buyer_uid, b.seller_uid, b.seller_pid_price
from bunjang_promise b
join product_info_for_stats p
on b.seller_pid = p.pid
join categories c
on p.category_id = c.category
where b.status = 4 and b.seller_pid_price > 0

"""



"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/bunp_history.csv'

# macro parameters
# start_time = '2019-01-01 00:00:00'
# end_time = '2019-03-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

SELECT updated_at, buyer_uid, seller_uid, seller_pid_price
FROM bunjang_promise
WHERE status = 4 AND seller_pid_price > 0

"""


"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/pay_history.csv'

# macro parameters
# start_time = '2019-01-01 00:00:00'
# end_time = '2019-03-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

SELECT mast.deposit_done_date, mast.buyer_id, item.seller_id, mast.total_price
FROM (
    SELECT id, deposit_done_date, buyer_id, total_price
    FROM order_mast
    WHERE order_status_cd = 'purchase_confirm' AND deposit_done_date IS NOT NULL
) mast
JOIN order_item item
ON mast.id = item.order_mast_id
"""



"""
Query & Save Path
"""

# path to save
save_path = 'csv/products_cohort.csv'


# macro parameters
# start_time = '2019-04-01 00:00:00'
# end_time = '2019-05-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

-- 유저 셩, 연령별, 월별 등록, 판매, 삭제된 제품 수

WITH products AS (
SELECT date_trunc('month', create_date) AS enrolled_at,
    (CASE WHEN status = 1 THEN date_trunc('month', register_date) ELSE NULL END) AS booked_at, 
    (CASE WHEN status = 2 THEN date_trunc('month', register_date) ELSE NULL END) AS deleted_at, 
    (CASE WHEN status = 3 THEN date_trunc('month', register_date) ELSE NULL END) AS sold_at, 
    LEFT(category_id, 6) AS category_id, status, uid, 
    count(DISTINCT id) AS products
FROM product_info_for_stats
WHERE create_date >= '2019-01-01 00:00:00' AND create_date <= '2019-05-01 00:00:00'
GROUP BY 1, 2, 3, 4, 5, 6, 7
)


SELECT p.enrolled_at, p.booked_at, p.deleted_at, p.sold_at,
    p.category_id, c.name, p.status, u.age, u.gender, SUM(p.products) AS products
FROM products p
LEFT JOIN user_for_stats u
ON p.uid = u.uid
LEFT JOIN categories c
ON p.category_id = c.category
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9



"""


"""
Query & Save Path
"""

# path to save
save_path = 'csv/bunp_cohort_since_1901.csv'


# macro parameters
# start_time = '2019-04-01 00:00:00'
# end_time = '2019-05-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """


-- 카테고리별 셀러, 바이어의 성, 연령별 번프 완료된 상품 수, 금액


-- 전체 구매 데이터
WITH bunp_all AS (
SELECT date_trunc('month', updated_at) AS month_at, seller_uid, buyer_uid, seller_pid, seller_pid_price AS bunp_amount
FROM bunjang_promise
WHERE status = 4 AND seller_pid_price > 0 AND seller_pid_price < 3000000 AND 
    updated_at >= '2019-01-01' AND updated_at < '2019-05-01'
),
bunp_cate AS (
SELECT LEFT(p.category_id, 6) AS category_id, a.month_at, 
    s.age AS seller_age, s.gender AS seller_gender, b.age AS buyer_age, b.gender AS buyer_gender,
    count(a.seller_pid) AS selled_count, sum(a.bunp_amount) AS bunp_amount
FROM bunp_all a
LEFT JOIN product_info_for_stats p
ON a.seller_pid = p.pid
LEFT JOIN user_for_stats s
ON s.uid = a.seller_uid
LEFT JOIN user_for_stats b
ON b.uid = a.buyer_uid
WHERE p.create_date >= '2019-01-01'
GROUP BY 1, 2, 3, 4, 5, 6
)


--ㅡ구매 상품의 카테고리 이름
SELECT b.category_id, c.name, b.month_at, 
    b.seller_age, b.seller_gender, b.buyer_age, b.buyer_gender,   
    b.selled_count, b.bunp_amount
FROM bunp_cate b
LEFT JOIN categories c
ON b.category_id = c.category



"""


"""
Query & Save Path
"""

# path to save
save_path = 'csv/bunp_cohort.csv'


# macro parameters
# start_time = '2019-04-01 00:00:00'
# end_time = '2019-05-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """


-- 카테고리별 셀러, 바이어의 성, 연령별 번프 완료된 상품 수, 금액


-- 전체 구매 데이터
WITH bunp_all AS (
SELECT date_trunc('month', updated_at) AS month_at, seller_uid, buyer_uid, seller_pid, seller_pid_price AS bunp_amount
FROM bunjang_promise
WHERE status = 4 AND seller_pid_price > 0 AND seller_pid_price < 3000000 AND 
    updated_at >= '2019-01-01' AND updated_at < '2019-05-01'
),
bunp_cate AS (
SELECT LEFT(p.category_id, 6) AS category_id, a.month_at, 
    s.age AS seller_age, s.gender AS seller_gender, b.age AS buyer_age, b.gender AS buyer_gender,
    count(a.seller_pid) AS selled_count, sum(a.bunp_amount) AS bunp_amount
FROM bunp_all a
LEFT JOIN product_info_for_stats p
ON a.seller_pid = p.pid
LEFT JOIN user_for_stats s
ON s.uid = a.seller_uid
LEFT JOIN user_for_stats b
ON b.uid = a.buyer_uid
GROUP BY 1, 2, 3, 4, 5, 6
)


--ㅡ구매 상품의 카테고리 이름
SELECT b.category_id, c.name, b.month_at, 
    b.seller_age, b.seller_gender, b.buyer_age, b.buyer_gender,   
    b.selled_count, b.bunp_amount
FROM bunp_cate b
LEFT JOIN categories c
ON b.category_id = c.category



"""









"""
Query & Save Path
"""

# path to save
save_path = 'csv/keyword_category_1901_1904.csv'

# macro parameters
start_time = '2019-01-01 00:00:00'
end_time = '2019-05-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s


# query to run
query = """

-- 클릭된 상품의 검색 키워드와 세부 카테고리

WITH keyword_category AS (
SELECT tmp.keyword, c.category, c.name, COUNT(*) AS _count
FROM (
    SELECT i.keyword, category_id
    FROM item_click_log6 i
    LEFT JOIN product_info_for_stats p
    ON i.target_id = p.pid
    WHERE i.updated >= %(start_time)s AND i.updated < %(end_time)s AND
        i.keyword <> ''
) tmp
LEFT JOIN categories c
ON tmp.category_id = c.category
GROUP BY 1, 2, 3)


SELECT *
FROM (
    SELECT keyword, category, name, _count,
        ROW_NUMBER () OVER (PARTITION by keyword ORDER BY _count DESC) AS _num
    FROM keyword_category
)
WHERE _num = 1


"""




"""
Query & Save Path
"""

# path to save
save_path = 'csv/keywords_cohort.csv'


# macro parameters
start_time = '2019-01-01 00:00:00'
end_time = '2019-05-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

-- 월별 키워드, 성, 연령 별 검색 수

SELECT date_trunc('month', s.updated) AS month_at, s.keyword, u.age, u.gender, COUNT(s.id) AS search_num
FROM item_search_log s
LEFT JOIN user_for_stats u
ON s.viewer_uid = u.uid
where s.updated >= %(start_time)s AND s.updated < %(end_time)s
GROUP BY 1, 2, 3, 4

"""


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
SELECT date_trunc('day', updated_at) AS updated_at, seller_uid, seller_pid, seller_pid_price AS bunp_amount
FROM bunjang_promise
WHERE status = 4 AND seller_pid_price > 0 AND seller_pid_price < 3000000 AND 
    updated_at >= '2018-01-01' AND updated_at < '2019-05-01'
),
-- 구매 상품의 카테고리 id, 18년 1월 1일 이후 등록된 상품만
bunp_cate AS (
SELECT p.category_id, a.updated_at, p.p_biz, count(a.seller_pid) AS selled_count, sum(a.bunp_amount) AS bunp_amount
FROM bunp_all a
JOIN product_info_for_stats p
ON a.seller_pid = p.pid
WHERE p.create_date >= '2018-01-01'
GROUP BY 1, 2, 3
)


--ㅡ구매 상품의 카테고리 이름
SELECT c.category, c.name, b.updated_at, b.p_biz, b.selled_count, b.bunp_amount
FROM bunp_cate b
LEFT JOIN categories c
ON b.category_id = c.category



"""




"""
Query & Save Path
"""

# path to save
save_path = 'csv/clicked_cohort.csv'


# macro parameters
start_time = '2019-01-01 00:00:00'
end_time = '2019-05-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

-- 월별 카테고리, 성, 연령 별 상품 클릭 수

SELECT tmp.month_at, tmp.category_id, c.name, tmp.age, tmp.gender, tmp.clicked
FROM (
    SELECT date_trunc('month', c.updated) AS month_at, p.category_id, u.age, u.gender, COUNT(c.id) AS clicked
    FROM item_click_log6 c
    LEFT JOIN user_for_stats u
    ON c.viewer_uid = u.uid
    LEFT JOIN product_info_for_stats p
    ON c.target_id = p.pid
    WHERE c.updated >= %(start_time)s AND c.updated < %(end_time)s
    GROUP BY 1, 2, 3, 4
) tmp
LEFT JOIN categories c
ON tmp.category_id = c.category


"""



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

-- 일별 카테고리별 상품 클릭 수

SELECT tmp.date_at, tmp.category_id, c.name, tmp.p_biz, tmp.clicked
FROM (
    SELECT date_trunc('day', i.updated) AS date_at, p.category_id, p.p_biz, COUNT(i.id) AS clicked
    FROM item_click_log6 i
    LEFT JOIN product_info_for_stats p
    ON i.target_id = p.pid
    WHERE i.updated >= %(start_time)s AND i.updated < %(end_time)s
    GROUP BY 1, 2, 3
) tmp
LEFT JOIN categories c
ON tmp.category_id = c.category


"""




"""
Query & Save Path
"""

# path to save
save_path = 'csv/products.csv'


# macro parameters
# start_time = '2019-04-01 00:00:00'
# end_time = '2019-05-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

-- 일별 등록, 판매, 삭제된 제품 수
SELECT date_trunc('day', create_date) AS on_at,
    (CASE WHEN status > 0 THEN date_trunc('day', register_date) ELSE NULL END) AS close_at, 
    category_id, status, p_biz,
    count(DISTINCT id) AS products
FROM product_info_for_stats
WHERE create_date >= '2017-01-01 00:00:00' AND create_date <= '2019-05-01 00:00:00'
GROUP BY 1, 2, 3, 4, 5


"""



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
SELECT date_trunc('day', updated_at) AS updated_at, seller_uid, seller_pid, seller_pid_price AS bunp_amount
FROM bunjang_promise
WHERE status = 4 AND seller_pid_price > 0 AND seller_pid_price < 3000000 AND 
    updated_at >= '2018-01-01' AND updated_at < '2019-05-01'
),
-- 구매 상품의 카테고리 id, 18년 1월 1일 이후 등록된 상품만
bunp_cate AS (
SELECT p.category_id, a.updated_at, p.p_biz, count(a.seller_pid) AS selled_count, sum(a.bunp_amount) AS bunp_amount
FROM bunp_all a
JOIN product_info_for_stats p
ON a.seller_pid = p.pid
WHERE p.create_date >= '2018-01-01'
GROUP BY 1, 2, 3
)


--ㅡ구매 상품의 카테고리 이름
SELECT c.category, c.name, b.updated_at, b.p_biz, b.selled_count, b.bunp_amount
FROM bunp_cate b
LEFT JOIN categories c
ON b.category_id = c.category



"""




"""
Query & Save Path
"""

# path to save
save_path = 'csv/keywords_1801_1806.csv'


# macro parameters
start_time = '2018-01-01 00:00:00'
end_time = '2018-07-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

-- 일별 키워드별 검색 수
SELECT date_trunc('day', updated) AS date_at, keyword, COUNT(id) AS search_num
FROM item_search_log
where updated >= %(start_time)s AND updated < %(end_time)s
GROUP BY 1, 2

"""






"""
Query & Save Path
"""

# path to save
save_path = 'csv/bunp_category_detail_month.csv'


# macro parameters
# start_time = '2019-04-01 00:00:00'
# end_time = '2019-05-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """




-- 전체 구매 데이터
WITH bunp_all AS (
SELECT seller_pid, date_trunc('month', updated_at) AS updated_at, seller_pid_price AS bunp_amount
FROM bunjang_promise
WHERE status = 4 AND seller_pid_price > 0 AND seller_pid_price < 3000000 AND 
    updated_at >= '2017-01-01' AND updated_at < date_trunc('month', CURRENT_DATE)
),
-- 구매 상품의 카테고리 id
bunp_cate AS (
SELECT category_id AS cate_id, a.updated_at, a.seller_pid, a.bunp_amount
FROM bunp_all a
LEFT JOIN product_info_for_stats p
ON a.seller_pid = p.pid
)

--ㅡ구매 상품의 카테고리 이름
SELECT c.category, c.name, b.updated_at, b.seller_pid, b.bunp_amount
FROM bunp_cate b
LEFT JOIN categories c
ON b.cate_id = c.category



"""










"""
Query & Save Path
"""

# path to save
save_path = 'csv/bunp_category_detail.csv'


# macro parameters
# start_time = '2019-04-01 00:00:00'
# end_time = '2019-05-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """


-- 구매 상품의 카테고리

-- 전체 구매 데이터
WITH bunp_all AS (
SELECT seller_pid, updated_at, seller_pid_price AS bunp_amount
FROM bunjang_promise
WHERE status = 4 AND seller_pid_price > 0 AND seller_pid_price < 3000000 AND 
    updated_at >= '2017-01-01' AND updated_at < date_trunc('month', CURRENT_DATE)
),
-- 구매 상품의 카테고리 id
bunp_cate AS (
SELECT category_id AS cate_id, a.updated_at, a.seller_pid, a.bunp_amount
FROM bunp_all a
LEFT JOIN product_info_for_stats p
ON a.seller_pid = p.pid
)

--ㅡ구매 상품의 카테고리 이름
SELECT c.category, c.name, b.updated_at, b.seller_pid, b.bunp_amount
FROM bunp_cate b
LEFT JOIN categories c
ON b.cate_id = c.category



"""







"""
Query & Save Path
"""

# path to save
save_path = 'csv/bunp_category.csv'


# macro parameters
# start_time = '2019-04-01 00:00:00'
# end_time = '2019-05-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """




-- 전체 구매 데이터
WITH bunp_all AS (
SELECT seller_pid, date_trunc('month', updated_at) AS month_at, 
    sum(seller_pid_price) AS bunp_amount
FROM bunjang_promise
WHERE status = 4 AND seller_pid_price > 0 AND seller_pid_price < 3000000 AND 
    updated_at >= '2017-01-01' AND updated_at < date_trunc('month', CURRENT_DATE)
GROUP BY 1, 2
),
-- 구매 상품의 카테고리 id
bunp_cate AS (
SELECT category_id AS cate_id, a.month_at, a.bunp_amount
FROM bunp_all a
LEFT JOIN product_info_for_stats p
ON a.seller_pid = p.pid
)

--ㅡ구매 상품의 카테고리 이름
SELECT c.category, c.name, to_char(month_at, 'YYYY-MM') AS month_at, 
    SUM(b.bunp_amount) AS bunp_amount
FROM bunp_cate b
LEFT JOIN categories c
ON b.cate_id = c.category
GROUP BY 1, 2, 3




"""



"""
Query & Save Path
"""

# path to save
save_path = 'csv/keyword_search_user.csv'


# macro parameters
start_time = '2019-04-01 00:00:00'
end_time = '2019-05-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run

query = """

-- 19년 4월 검색한 유저의 정보

SELECT i.viewer_uid, s.device, s.age, s.gender, s.bizlicense
FROM (
SELECT distinct viewer_uid
FROM item_search_log
WHERE updated >= %(start_time)s AND updated < %(end_time)s
) i 
LEFT JOIN user_for_stats s
ON i.viewer_uid = s.uid


"""




"""
Query & Save Path
"""

# path to save
save_path = 'csv/keyword_category_detail.csv'

# query to run
query = """

-- 19년 4월 클릭된 상품의 검색 키워드와 세부 카테고리

WITH keyword_category AS (
SELECT tmp.keyword, c.category, c.name, COUNT(*) AS _count
FROM (
    SELECT i.keyword, category_id
    FROM item_click_log6 i
    LEFT JOIN product_info_for_stats p
    ON i.target_id = p.pid
    WHERE i.updated >= '2019-04-01' AND i.updated < '2019-05-01' AND
        i.keyword <> '' AND p.category_id <> ''
) tmp
LEFT JOIN categories c
ON tmp.category_id = c.category
GROUP BY 1, 2, 3)


SELECT *
FROM (
    SELECT keyword, category, name, _count,
        ROW_NUMBER () OVER (PARTITION by keyword ORDER BY _count DESC) AS _num
    FROM keyword_category
)
WHERE _num = 1


"""



"""
Query & Save Path
"""

# path to save
save_path = 'csv/keyword_category.csv'

# query to run
query = """

-- 19년 4월 클릭된 상품의 검색 키워드와 카테고리

WITH keyword_category AS (
SELECT tmp.keyword, c.category, c.name, COUNT(*) AS _count
FROM (
    SELECT i.keyword, left(p.category_id, 3) AS category_id
    FROM item_click_log6 i
    LEFT JOIN product_info_for_stats p
    ON i.target_id = p.pid
    WHERE i.updated >= '2019-04-01' AND i.updated < '2019-05-01' AND
        i.keyword <> '' AND p.category_id <> ''
) tmp
LEFT JOIN categories c
ON tmp.category_id = c.category
GROUP BY 1, 2, 3)


SELECT *
FROM (
    SELECT keyword, category, name, _count,
        ROW_NUMBER () OVER (PARTITION by keyword ORDER BY _count DESC) AS _num
    FROM keyword_category
)
WHERE _num = 1


"""




"""
Query & Save Path
"""

# path to save
save_path = 'csv/keyword_click.csv'


# macro parameters
start_time = '2019-04-01 00:00:00'
end_time = '2019-05-01 00:00:00'


# query to run

query = """

-- 19년 4월 클릭된 상품의 카테고리와 유입된 키워드
-- 매크로

SELECT i.viewer_uid, i.viewer_device, i.keyword, p.pid, c.category, c.name,
    COUNT(i.id) AS count_click
FROM item_click_log6 i
LEFT JOIN product_info_for_stats p
ON i.target_id = p.pid
LEFT JOIN categories c
ON p.category_id = c.category
WHERE i.updated >= %(start_time)s AND i.updated < %(end_time)s AND
    i.keyword <> ''
GROUP BY 1, 2, 3, 4, 5, 6

"""










"""
Query & Save Path
"""

# path to save
save_path = 'csv/keyword_category_direct.csv'


# query to run

query = """

-- 클릭된 상품의 카테고리와 유입된 키워드 쿼리

SELECT *
FROM (
SELECT tmp.keyword, c.category, c.name, COUNT(*) AS _count,
    ROW_NUMBER () OVER (PARTITION by keyword ORDER BY _count DESC) AS _num
FROM (
SELECT i.keyword, left(p.category_id, 3) AS category_id
FROM item_click_log6 i
LEFT JOIN product_info_for_stats p
ON i.target_id = p.pid
WHERE i.updated >= '2019-04-01' AND i.updated < '2019-05-01' AND
    i.keyword <> '' AND p.category_id <> ''
) tmp
LEFT JOIN categories c
ON tmp.category_id = c.category
GROUP BY 1, 2, 3
)
WHERE _num = 1

"""








"""
Query & Save Path
"""

# path to save
save_path = 'csv/keyword_search.csv'


# macro parameters
# start_time = '2019-04-01 00:00:00'
# end_time = '2019-05-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

-- 19년 4월 검색한 키워드의 상세 검색 활용 여부

SELECT viewer_uid, viewer_device, keyword,
    SUM(CASE WHEN location <> '' THEN COUNT_search ELSE 0 END) AS COUNT_location,
    SUM(CASE WHEN category <> '' THEN COUNT_search ELSE 0 END) AS COUNT_category,
    SUM(CASE WHEN price_min <> '' THEN COUNT_search ELSE 0 END) AS COUNT_price_min,
    SUM(CASE WHEN price_max <> '' THEN COUNT_search ELSE 0 END) AS COUNT_price_max,
    SUM(CASE WHEN used <> '' THEN COUNT_search ELSE 0 END) AS COUNT_used,
    SUM(CASE WHEN bizseller <> '' THEN COUNT_search ELSE 0 END) AS COUNT_bizseller,
    SUM(CASE WHEN FREE_shipping <> '' THEN COUNT_search ELSE 0 END) AS COUNT_free_shipping,
    SUM(CASE WHEN exchg <> '' THEN COUNT_search ELSE 0 END) AS COUNT_exchg,
    SUM(CASE WHEN location = '' AND 
                category = '' AND
                price_min = '' AND
                price_max = '' AND
                used = '' AND
                bizseller = '' AND
                FREE_shipping = '' AND
                exchg = '' THEN COUNT_search ELSE 0 END) AS COUNT_none,
    SUM(CASE WHEN location <> '' OR 
                category <> '' OR
                price_min <> '' OR
                price_max <> '' OR
                used <> '' OR
                bizseller <> '' OR
                FREE_shipping <> '' OR
                exchg <> '' THEN COUNT_search ELSE 0 END) AS COUNT_detail
FROM (
SELECT viewer_uid, viewer_device, keyword, 
    json_extract_path_text(search_option_json, 'location[]') AS location,
    json_extract_path_text(search_option_json, 'category_id') AS category,
    json_extract_path_text(search_option_json, 'price_min') AS price_min,
    json_extract_path_text(search_option_json, 'price_max') AS price_max,
    json_extract_path_text(search_option_json, 'used') AS used,
    json_extract_path_text(search_option_json, 'bizseller') AS bizseller,
    json_extract_path_text(search_option_json, 'free_shipping') AS free_shipping,
    json_extract_path_text(search_option_json, 'exchg') AS exchg,
    COUNT(id) AS COUNT_search
FROM item_search_log
WHERE updated >= '2019-04-01' AND updated < '2019-05-01' AND
    json_extract_path_text(search_option_json, 'req_ref') <> 'neighborhood'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
)
GROUP BY 1, 2, 3


"""






"""
Query & Save Path
"""

# path to save
save_path = 'csv/keywords_category_cohort.csv'


# macro parameters
start_time = '2019-01-01 00:00:00'
end_time = '2019-05-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """


-- 월별 카테고리, 키워드, 성, 연령 별 검색 수

WITH keyword AS (
SELECT date_trunc('month', c.updated) AS month_at, c.keyword, LEFT(p.category_id, 6) AS category_id, u.age, u.gender, 
    COUNT(c.id) AS search_count
FROM item_click_log6 c
LEFT JOIN user_for_stats u
ON c.viewer_uid = u.uid
LEFT JOIN product_info_for_stats p
ON c.target_id = p.pid
WHERE c.updated >= %(start_time)s AND c.updated < %(end_time)s
GROUP BY 1, 2, 3, 4, 5
)

SELECT k.month_at, k.keyword, k.category_id, c.name, k.age, k.gender, k.search_count
FROM keyword k
LEFT JOIN categories c
ON k.category_id = c.category


"""


