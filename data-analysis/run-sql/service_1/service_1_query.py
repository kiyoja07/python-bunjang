

"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/reward_test_product_191107_191113.csv'

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
where p.create_date between '2019-11-07 00:00:00' and '2019-11-13 23:59:59' # 상품 등록 기간


"""




# ---------------------------------------------------------------------------------
