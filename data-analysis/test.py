from read_redshift import connect_redshift


query = """
select id
from user_join_log
where date(updated) = '2019-05-01'
"""

print(connect_redshift(query))
