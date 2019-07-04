



# path to save
save_path = 'csv/review_writer.csv'


# macro parameters
# start_time = '2019-01-01 00:00:00.000000'
# end_time = '2019-01-03 00:00:00.000000'

# updated >= %(start_time)s AND updated < %(end_time)s

# query to run
query = """

select time, writer_uid
from user_review

"""

