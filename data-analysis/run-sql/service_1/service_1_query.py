




"""
DB : Service 1, MySQL
Query & Save Path
"""


# path to save
save_path = 'csv/reward_pay_1.0_pay_fail.csv'

# macro parameters
# start_time = '2019-10-22 00:00:00.0'  # 이상
# end_time = '2019-11-01 00:00:00.0'  # 미만

interval_type = None
# interval_type = 'days'
# interval_type = 'months'


# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

-- 번개 송금으로 결제
select uid, paid_at, 'transfer' as pay_type, status
from wire_transfer
where date_format(paid_at, '%Y-%m-%d') between '2019-12-18' and '2020-01-03' and # 결제 기간
    status not in ('payment_received', 'verifying_delivery', 'transfer_completed')
    # 가상계좌 입금 완료(운송장 번호 입력 대기), 송장번호 입력완료(배송확인중), 송금완료(운송장 검증 완료)

union all

-- 번개 페이로 결제
select buyer_id, setl_done_date, 'pay' as pay_type, order_status_cd
from order_mast
where date_format(setl_done_date, '%Y-%m-%d') between '2019-12-18' and '2020-01-03' and # 결제 기간
    order_status_cd not in ('payment_received', 'ship_ready', 'in_transit', 'delivery_completed', 'purchase_confirm') 
    # 결제완료, 배송 준비 중, 배송 중, 배송 완료, 거래완료


"""


# ---------------------------------------------------------------------------------

