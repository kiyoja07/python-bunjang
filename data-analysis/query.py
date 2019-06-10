


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


SELECT date_trunc('day', updated) AS date_at, keyword, COUNT(id) AS search_num
FROM item_search_log
where updated >= %(start_time)s AND updated < %(end_time)s
GROUP BY 1, 2

"""