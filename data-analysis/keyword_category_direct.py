from connect_db import connect_redshift_direct
from query import query


def run_query(query, save_path):


    result = connect_redshift_direct(query)

    result.to_csv(save_path, index=False, mode='w', header=True)

    print('Done')

    return True


if __name__ == "__main__":

    query = query

    save_path = 'csv/keyword_category_direct.csv'

    run_query(query, save_path)
