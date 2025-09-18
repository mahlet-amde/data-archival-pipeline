import pymysql
from collections import defaultdict
import re


def clean_column_name(name):
    prefixes = ['auctions_', 'facilities_', 'auction_bids_', 'invoices_', 'auction_images_']
    for prefix in prefixes:
        if name.startswith(prefix):
            return name[len(prefix):]
    return name

def process_multiple_results(cursor):
    results = defaultdict(list)
    
    try:
        data = cursor.fetchall()
        if data:
            first_key = list(data[0].keys())[0]
            data_type = re.search(r'^[a-zA-Z_]+(?=_)', first_key)
            if data_type:
                data_type = data_type.group(0)
                for row in data:
                    clean_row = {clean_column_name(k): v for k, v in row.items()}
                    results[data_type].append(clean_row)

        while cursor.nextset():
            data = cursor.fetchall()
            if data:
                first_key = list(data[0].keys())[0]
                data_type = re.search(r'^[a-zA-Z_]+(?=_)', first_key)
                if data_type:
                    data_type = data_type.group(0)
                    for row in data:
                        clean_row = {clean_column_name(k): v for k, v in row.items()}
                        results[data_type].append(clean_row)
    except pymysql.MySQLError as e:
        print(f"Error processing result sets: {e}")
    
    return results