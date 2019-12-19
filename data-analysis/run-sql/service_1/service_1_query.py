




"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/reward_register_2.0_product.csv'

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
        and date_format(u.join_date, '%Y-%m-%d') in ('2019-12-04', '2019-12-05', '2019-12-08') # 이벤트 대상 가입일
) u
join product_info p
on u.uid = p.uid
where p.create_date between '2019-12-05 00:00:00' and '2019-12-16 23:59:59' # 상품 등록 기간


"""


# ---------------------------------------------------------------------------------
# where u.update_time between '2019-10-22 00:00:00' and '2019-10-31 23:59:59' # UP 실행 기간
