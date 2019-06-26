
"""
DB : Redshift, PostgreSQL
Query & Save Path
"""


# path to save
save_path = 'csv/test.csv'


# macro parameters
# start_time = '2019-01-01 00:00:00'
# end_time = '2019-05-01 00:00:00'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

-- 월별 키워드, 성, 연령 별 검색 수

select *
from categories

"""
