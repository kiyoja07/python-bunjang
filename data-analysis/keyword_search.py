from connect_db import connect_redshift_direct



def run_query(query, save_path):


    result = connect_redshift_direct(query)

    result.to_csv(save_path, index=False, mode='w', header=True)

    print('Done')

    return True



if __name__ == "__main__":

    query = """
 
SELECT keyword,
    SUM(CASE WHEN location <> '' THEN COUNT_search ELSE 0 END) AS COUNT_location,
    SUM(CASE WHEN category <> '' THEN COUNT_search ELSE 0 END) AS COUNT_category,
    SUM(CASE WHEN price_min <> '' THEN COUNT_search ELSE 0 END) AS COUNT_price_min,
    SUM(CASE WHEN price_max <> '' THEN COUNT_search ELSE 0 END) AS COUNT_price_max,
    SUM(CASE WHEN used <> '' THEN COUNT_search ELSE 0 END) AS COUNT_used,
    SUM(CASE WHEN bizseller <> '' THEN COUNT_search ELSE 0 END) AS COUNT_bizseller,
    SUM(CASE WHEN FREE_shipping <> '' THEN COUNT_search ELSE 0 END) AS COUNT_free_shipping,
    SUM(CASE WHEN exchg <> '' THEN COUNT_search ELSE 0 END) AS COUNT_exchg,
    SUM(CASE WHEN location = '' AND 
                category = '' AND
                price_min = '' AND
                price_max = '' AND
                used = '' AND
                bizseller = '' AND
                FREE_shipping = '' AND
                exchg = '' THEN COUNT_search ELSE 0 END) AS COUNT_none
FROM (
SELECT keyword, 
    json_extract_path_text(search_option_json, 'location[]') AS location,
    json_extract_path_text(search_option_json, 'category_id') AS category,
    json_extract_path_text(search_option_json, 'price_min') AS price_min,
    json_extract_path_text(search_option_json, 'price_max') AS price_max,
    json_extract_path_text(search_option_json, 'used') AS used,
    json_extract_path_text(search_option_json, 'bizseller') AS bizseller,
    json_extract_path_text(search_option_json, 'free_shipping') AS free_shipping,
    json_extract_path_text(search_option_json, 'exchg') AS exchg,
    COUNT(id) AS COUNT_search
FROM item_search_log
WHERE updated >= '2019-04-01' AND updated < '2019-05-01'
GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
)
GROUP BY 1

     """

    save_path = 'csv/keyword_search.csv'

    run_query(query, save_path)
