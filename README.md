# Auction Data Archiving Project
## Project Overview
This project is a Proof of Concept (POC) for an automated data archiving solution. The primary goal is to move old, infrequently accessed auction data from a live transactional database to a more cost-effective, long-term storage solution in Amazon S3.
The process is orchestrated by a main script that pulls data from multiple tables, transforms it into a single JSON object per auction, and archives it in a partitioned S3 bucket.

## Data Flow
1. The data moves through a clear pipeline:
2. The .env file loads credentials for security.
3. The data_archival.py script uses database_connection.py to connect to your database.It pulls all related data from your tables and feeds it to data_processor.py.
4. The data_processor.py script transforms the data into the final JSON structure.
5. Finally, data_archival.py uses the s3_manager.py module to upload the JSON file to your S3 bucket.

## Getting Started
Configure .env: Add your database and S3 credentials to the .env file.
Run the Archival Script: Execute the data_archival.py script to start the archiving process.