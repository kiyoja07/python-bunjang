
"""
Query Archive
"""





"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/reward_test_up_1022_1031.csv'

# macro parameters
# start_time = '2019-10-22 00:00:00.0'  # 이상
# end_time = '2019-11-01 00:00:00.0'  # 미만

interval_type = None
# interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select p.uid, p.pid, u.register_at
from (
    select u.uid, p.id as pid
    from (
        select distinct u.id as uid
        from user u
        where u.status = 0 # 탈퇴 유저 제외
            and u.join_date between '2019-10-21 00:00:00' and '2019-10-23 23:59:59' # 이벤트 대상 가입일
    ) u
    join product_info p
    on u.uid = p.uid
    where p.create_date between '2019-10-22 00:00:00' and '2019-10-31 23:59:59' # 상품 등록 기간
) p
join up_count_history u
on p.pid = u.pid


"""





"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/reward_test_bunp_1114_1120.csv'

# macro parameters
start_time = '2019-11-14 00:00:00.0'  # 이상
end_time = '2019-11-21 00:00:00.0'  # 미만

# interval_type = None
interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select channel_id, seller_uid, buyer_uid, seller_pid, created_at
from bunjang_promise
where created_at >= %(start_time)s AND created_at < %(end_time)s


"""


"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/reward_test_share_191022_191120.csv'

# macro parameters
# start_time = '2019-10-22 00:00:00.0'  # 이상
# end_time = '2019-11-01 00:00:00.0'  # 미만

interval_type = None
# interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select u.uid, s.date, s.pid, s.type
from (
    select distinct u.id as uid
    from user u
    where u.status = 0 # 탈퇴 유저 제외
        and u.join_date between '2019-10-21 00:00:00' and '2019-10-23 23:59:59' # 이벤트 대상 가입일
) u
join shared_log s
on u.uid = s.sharer_uid
where s.date between '2019-10-22 00:00:00' and '2019-11-20 23:59:59' # 공유 기간


"""





"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/reward_test_product_191114_191120.csv'

# macro parameters
# start_time = '2019-10-22 00:00:00.0'  # 이상
# end_time = '2019-11-01 00:00:00.0'  # 미만

interval_type = None
# interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select u.uid, p.create_date, p.id as pid
from (
    select distinct u.id as uid
    from user u
    where u.status = 0 # 탈퇴 유저 제외
        and u.join_date between '2019-10-21 00:00:00' and '2019-10-23 23:59:59' # 이벤트 대상 가입일
) u
join product_info p
on u.uid = p.uid
where p.create_date between '2019-11-14 00:00:00' and '2019-11-20 23:59:59' # 상품 등록 기간


"""



"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/product_keyword_1911.csv'

# macro parameters
# start_time = '2019-10-22 00:00:00.0'  # 이상
# end_time = '2019-11-01 00:00:00.0'  # 미만

interval_type = None
# interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select id as pid, create_date, keyword
from product_info
where create_date between '2019-11-01 00:00:00' and '2019-11-07 23:59:59'

"""







"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/reward_test_product_191107_191113.csv'

# macro parameters
# start_time = '2019-10-22 00:00:00.0'  # 이상
# end_time = '2019-11-01 00:00:00.0'  # 미만

# interval_type = None
# interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select u.uid, p.create_date, p.id as pid
from (
    select distinct u.id as uid
    from user u
    where u.status = 0 # 탈퇴 유저 제외
        and u.join_date between '2019-10-21 00:00:00' and '2019-10-23 23:59:59' # 이벤트 대상 가입일
) u
join product_info p
on u.uid = p.uid
where p.create_date between '2019-11-07 00:00:00' and '2019-11-13 23:59:59' # 상품 등록 기간


"""








"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/product_description.csv'

# macro parameters
# start_time = '2018-01-01 00:00:00'  # 이상
# end_time = '2019-07-01 00:00:00'  # 미만

# interval_type = 'months'
interval_type = None

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select pid, 1 as description
from product_description

"""

# ---------------------------------------------------------------------------------




"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/changed_name_raw.csv'


# macro parameters
# start_time = '2019-01-01 00:00:00.000000'
# end_time = '2019-01-03 00:00:00.000000'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """


select user.id, name.date as change_name_date, user.join_date
from user_name_history as name
left join user
on name.uid = user.id
where user.join_date <= '2019-06-30'

union

select user.id, name.date as change_name_date, user.join_date
from user_name_history as name
right join user
on name.uid = user.id
where user.join_date <= '2019-06-30'

"""




"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/block_uid.csv'


# query to run
query = """



-- 차단 사용자 목록
SELECT DISTINCT uid
FROM 
(
SELECT uid
FROM user_auth_token
WHERE policy_id=2

UNION ALL

SELECT ud.uid
FROM user_device AS ud
  LEFT OUTER JOIN device_group_exception AS dge ON ud.udid_str=dge.udid AND ud.uid=dge.uid
  LEFT OUTER JOIN (SELECT gps.udid
                   FROM device_group_block_policy_status AS gps
                     JOIN block_history_device_group AS gh ON gps.udid=gh.udid
                   WHERE gps.policy_id=2
                     AND gh.is_active=1
                     AND gh.policy_type1_category IN ('거래사기')) AS blocked_udid ON ud.udid_str=blocked_udid.udid
WHERE ud.`status` = 0
  AND ud.udid_str != ''
  AND dge.uid IS NULL
  AND blocked_udid.udid IS NOT NULL
) AS t

"""



"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/review_er.csv'


# query to run
query = """

select case when f.uid is null then 'normal'
	else 'black' end as is_black, n.writer_uid,
	count(*) as review
from user_review n
left join (
SELECT DISTINCT uid
FROM 
(
SELECT uid
FROM user_auth_token
WHERE policy_id=2

UNION ALL

SELECT ud.uid
FROM user_device AS ud
  LEFT OUTER JOIN device_group_exception AS dge ON ud.udid_str=dge.udid AND ud.uid=dge.uid
  LEFT OUTER JOIN (SELECT gps.udid
                   FROM device_group_block_policy_status AS gps
                     JOIN block_history_device_group AS gh ON gps.udid=gh.udid
                   WHERE gps.policy_id=2
                     AND gh.is_active=1
                     AND gh.policy_type1_category IN ('거래사기')) AS blocked_udid ON ud.udid_str=blocked_udid.udid
WHERE ud.`status` = 0
  AND ud.udid_str != ''
  AND dge.uid IS NULL
  AND blocked_udid.udid IS NOT NULL
) AS t
) f
on n.writer_uid = f.uid
group by 1, 2

"""



"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/changed_name.csv'


# macro parameters
# start_time = '2019-01-01 00:00:00.000000'
# end_time = '2019-01-03 00:00:00.000000'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select case when f.uid is null then 'normal'
	else 'black' end as is_black, n.uid,
	count(*) as changed_name
from user_name_history n
left join (
SELECT DISTINCT uid
FROM 
(
SELECT uid
FROM user_auth_token
WHERE policy_id=2

UNION ALL

SELECT ud.uid
FROM user_device AS ud
  LEFT OUTER JOIN device_group_exception AS dge ON ud.udid_str=dge.udid AND ud.uid=dge.uid
  LEFT OUTER JOIN (SELECT gps.udid
                   FROM device_group_block_policy_status AS gps
                     JOIN block_history_device_group AS gh ON gps.udid=gh.udid
                   WHERE gps.policy_id=2
                     AND gh.is_active=1
                     AND gh.policy_type1_category IN ('거래사기')) AS blocked_udid ON ud.udid_str=blocked_udid.udid
WHERE ud.`status` = 0
  AND ud.udid_str != ''
  AND dge.uid IS NULL
  AND blocked_udid.udid IS NOT NULL
) AS t
) f
on n.uid = f.uid
group by 1, 2

"""



"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/wanna_buy.csv'


# macro parameters
# start_time = '2019-01-01 00:00:00.000000'
# end_time = '2019-01-03 00:00:00.000000'

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


# where i.create_date >= %(start_time)s AND i.create_date < %(end_time)s
# where i.create_date >= '2019-01-01 00:00:00' AND i.create_date < '2019-01-02 00:00:00'
