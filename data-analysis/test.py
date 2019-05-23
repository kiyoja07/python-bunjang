from connect_db import connect_redshift_direct
from query import query, save_path



def run_query(query, save_path):


    result = connect_redshift_direct(query)

    result.to_csv(save_path, index=False, mode='w', header=True)

    print('Done')

    return True



if __name__ == "__main__":


    run_query(query, save_path)
