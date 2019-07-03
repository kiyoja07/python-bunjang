from connect_db import *
from service_2_query import *
from datetime import *
from dateutil.relativedelta import relativedelta


def run_query_service_1_without_macro(query, save_path):

    result = connect_service_2_without_macro(query)

    result.to_csv(save_path, index=False, mode='w', header=True)

    print('query completed')

    return True




if __name__ == "__main__":

    print(query)

    run_query_service_1_without_macro(query, save_path)
