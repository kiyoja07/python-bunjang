
def save_query_result(save_path, result, query_count=0):
    """ 쿼리 결과를 csv로 저장 """

    if query_count == 0:
        result.to_csv(save_path, index=False, mode='w', header=True, encoding="utf-8") # encoding="euc-kr"
    else:
        result.to_csv(save_path, index=False, mode='a', header=False, encoding="utf-8")

    return None