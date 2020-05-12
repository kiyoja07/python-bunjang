import pandas as pd
import re
import csv

"""
import keyword & click
"""
file = open("./search_named_entity/csv/digital_200_no_space.csv", mode="r")
reader = csv.reader(file)
next(reader) # skip header
keyword_click = {rows[0]: rows[1] for rows in reader}

"""
import series
"""
file = open("./search_named_entity/csv/digital_series.csv", mode="r")
reader = csv.reader(file)
next(reader) # skip header

digital_series =[]
for rows in reader:
    parent = rows[0]
    name = rows[1]
    if re.search(parent, name):
        digital_series.append(name)
    else:
        digital_series.append(name)
        digital_series.append(str(parent+name))

series_set = set(digital_series)
digital_series = list(series_set)
digital_series.sort(key=len, reverse=True)


"""
import brand
"""
file = open("./search_named_entity/csv/digital_brand.csv", mode="r")
reader = csv.reader(file)
next(reader) # skip header

digital_brand =[]
for rows in reader:
    parent = rows[0]
    name = rows[1]
    if re.search(parent, name):
        digital_brand.append(name)
    else:
        digital_brand.append(name)
        digital_brand.append(str(parent+name))

brand_set = set(digital_brand)
digital_brand = list(brand_set)
digital_brand.sort(key=len, reverse=True)


"""
import category
"""
file = open("./search_named_entity/csv/keyword_category.csv", mode="r")
reader = csv.reader(file)
next(reader) # skip header

categories = [rows[0] for rows in reader]


categories_set = set(categories)
categories = list(categories_set)
categories.sort(key=len, reverse=True)


"""

"""
def find_entity(entity_list, term):
    for entity in entity_list:
        if bool(re.search(entity, term)):
            remained = term.replace(entity, '')
            if len(remained) == 0:
                return {"entity": entity, "remained": None}
            else:
                return {"entity": entity, "remained" : remained}
    return {"entity": None, "remained" : term}

for keyword in keyword_click.keys():
    # find a category
    category = find_entity(categories, keyword)['entity']
    remained = find_entity(categories, keyword)['remained']
    # find a series
    if remained is not None:
        series = find_entity(digital_series, remained)['entity']
        remained = find_entity(digital_series, remained)['remained']
    else:
        series = None
    # find a brand
    if remained is not None:
        brand = find_entity(digital_brand, remained)['entity']
        remained = find_entity(digital_brand, remained)['remained']
    else:
        brand = None
    if remained is not None:
        print(keyword, series, remained)


