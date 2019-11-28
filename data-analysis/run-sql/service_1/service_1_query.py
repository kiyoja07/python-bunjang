




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


# ---------------------------------------------------------------------------------
# where u.update_time between '2019-10-22 00:00:00' and '2019-10-31 23:59:59' # UP 실행 기간
