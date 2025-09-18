import pymysql
import sys

def connect_to_db(db_config):
    try:
        print("Connecting to the database...")
        conn = pymysql.connect(
            host=db_config['host'],
            user=db_config['user'],
            password=db_config['password'],
            db=db_config['dbname'],
            port=db_config['port'],
            cursorclass=pymysql.cursors.DictCursor
        )
        cursor = conn.cursor()
        print("Successfully connected to the database.")
        return conn, cursor

    except Exception as e:
        print(f"Error connecting to the database: {e}")
        sys.exit(1)