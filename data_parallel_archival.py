import json
import data_processor
import s3_manager
import database_connection
from datetime import datetime
import json
from collections import defaultdict
import os
import boto3
from dotenv import load_dotenv
from multiprocessing import Pool, cpu_count
from mysql.connector import Error

def minify_json(json_data):
    try:
        obj = json.loads(json_data)
        return json.dumps(obj, separators=(',', ':'))
    except json.JSONDecodeError as e:
        return f"Error: Invalid JSON data. {e}"

def process_single_auction(args):
    candidate_id, db_config = args # Unpack the arguments
    s3_bucket_name = "storage-treasure-archival"
    
    conn, cursor = database_connection.connect_to_db(db_config)
    if not cursor:
        print(f"Error: Could not connect to DB for candidate_id {candidate_id}")
        return

    try:
        print(f"Processing auction with candidate_id: {candidate_id}")
        cursor.callproc('xtivia_stage.generate_jsonqry2', (candidate_id, candidate_id))
        all_data = data_processor.process_multiple_results(cursor)
        json_data = {}
        
        for key, list_of_items in all_data.items():
            if key == 'auction':
                json_data['auction'] = list_of_items[0]
            if key == 'facility':
                json_data['facility'] = list_of_items[0]
            if key == 'bid':
                json_data['bids'] = list_of_items
            if key == 'auction_image':
                json_data['images'] = list_of_items
        
        closed_date_str = json_data['auction'].get('close_date')
        auction_id = json_data['auction'].get('auction_id')

        if closed_date_str:
            formatted_date = closed_date_str.strftime("year=%Y/month=%m")
            json_data_str = json.dumps(json_data, indent=4, default=str)
            minified_json = minify_json(json_data_str)
            s3_key = f"{formatted_date}/auction_{auction_id}.json"
            s3_manager.upload_file_to_s3(s3_bucket_name, s3_key, minified_json)
    
    except (Exception, Error) as e:
        print(f"An error occurred for candidate_id {candidate_id}: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()

def main():
    s3_bucket_name = "storage-treasure-archival"
    batch_size = 10
    ARCHIVE_DIR = 'archived_auctions'
    
    load_dotenv()
    
    db_config = {
        'dbname': os.environ.get('DB_NAME'),
        'user': os.environ.get('DB_USER'),
        'password': os.environ.get('DB_PASSWORD'),
        'host': os.environ.get('DB_HOST'),
        'port': int(os.environ.get('DB_PORT', 3306))
    }

    conn, cursor = database_connection.connect_to_db(db_config)
    if not cursor:
        return

    try:
        print("Populating auction candidates...")
        cursor.execute("CALL xtivia_stage.populate_auctions_candidates(10000)")
        
        min_max_query = "SELECT MIN(candidate_id) AS min_id, MAX(candidate_id) AS max_id FROM xtivia_stage.auctions_candidates;"
        cursor.execute(min_max_query)
        result = cursor.fetchone()
        min_id, max_id = result['min_id'], result['max_id']
        os.makedirs(ARCHIVE_DIR, exist_ok=True)
        
        if min_id is None or max_id is None:
            print("No auction candidates found.")
            return

        # Prepare the list of all IDs to process, paired with the db_config
        tasks = [(id, db_config) for id in range(min_id, max_id + 1)]
        
        # Use a multiprocessing Pool to distribute the work
        num_processes = cpu_count()
        print(f"Starting parallel processing with {num_processes} processes.")
        with Pool(processes=num_processes) as pool:
            pool.map(process_single_auction, tasks)

        print("\nArchiving process completed.")
    except (Exception, Error) as e:
        print(f"An error occurred during the archiving process: {e}")
    finally:
        if conn:
            cursor.close()
            conn.close()
            print("Database connection closed.")
            
if __name__ == "__main__":
    main()