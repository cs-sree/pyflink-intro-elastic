import sys
import argparse
import json
from typing import Iterable

from pyflink.table import TableEnvironment, EnvironmentSettings
from pyflink.table.descriptors import Schema
from pyflink.table.expressions import col, lit, call
from collections import Counter
import csv

# Schema Definition
SCHEMA = {
    "EdgeEndTimestamp": str,
    "ClientRequestURI": str,
    "EdgeResponseStatus": int,
    "ResponseHeaders": {
        "x-org-uid": str
    }
}

# Schema Validation Function
def validate_schema(record):
    try:
        if not isinstance(record, dict):
            return False
        if 'EdgeEndTimestamp' not in record or not isinstance(record['EdgeEndTimestamp'], str):
            return False
        if 'ClientRequestURI' not in record or not isinstance(record['ClientRequestURI'], str):
            return False
        if 'EdgeResponseStatus' not in record or not isinstance(record['EdgeResponseStatus'], int):
            return False
        if 'ResponseHeaders' not in record or not isinstance(record['ResponseHeaders'], dict):
            return False
        if 'x-org-uid' not in record['ResponseHeaders'] or not isinstance(record['ResponseHeaders']['x-org-uid'], str):
            return False
        return True
    except Exception as e:
        print(f"Schema validation error: {e}")
        return False


def main():
    # Initialize Table Environment
    env_settings = EnvironmentSettings.new_instance() \
        .in_batch_mode() \
        .build()
    table_env = TableEnvironment.create(env_settings)

    # Read the file and filter valid records
    file_path = '/Users/harshavardhan.reddy/Documents/ETL/pyflink-intro-elastic/apps/applied_practise/2021-03-27.json'
    with open(file_path, 'r') as f:
        json_data = [json.loads(line) for line in f if line.strip()]
    print(f"Loaded {len(json_data)} records from JSON file")

    # Filter only valid records based on the schema
    valid_data = [record for record in json_data if validate_schema(record)]
    print(f"Valid records found: {len(valid_data)}")

       # Flatten the nested 'x-org-uid'
    updated_data = []
    for entry in valid_data:
        flattened_entry = entry.copy()
        flattened_entry['x_org_uid'] = entry['ResponseHeaders'].get('x-org-uid', '')
        updated_data.append(flattened_entry)


    # Write valid data to a temp file
    temp_file = '/Users/harshavardhan.reddy/Documents/ETL/pyflink-intro-elastic/csv_output/valid_data.json'
    with open(temp_file, 'w') as temp_f:
        for entry in updated_data:
            temp_f.write(json.dumps(entry)+'\n')

    with open(temp_file, 'r') as f:
        pairs = [(json.loads(line)['x_org_uid'], json.loads(line)['ClientRequestURI']) for line in f]
    print(Counter(pairs))
    # Register the data as a Table
    table_env.execute_sql(f'''
        CREATE TEMPORARY TABLE Logs (
            EdgeEndTimestamp TIMESTAMP(3),
            ClientRequestURI STRING,
            EdgeResponseStatus INT,
            x_org_uid STRING
        ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///{temp_file}',
            'format' = 'json'
        )
    ''')

#  # Verify Data in Logs Table
#     print("--- Logs Table Data ---")
#     logs_result = table_env.sql_query("SELECT * FROM Logs")
#     with logs_result.execute().collect() as results:
#         for row in results:
#             print(row)

    # URL Count per Org
    print("--- URL Count Per Org ---")
    url_count = table_env.sql_query("""
        SELECT x_org_uid, ClientRequestURI, COUNT(*) AS url_count
        FROM Logs
        GROUP BY x_org_uid, ClientRequestURI
    """)
    
    with open('url_count_result.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['x_org_uid', 'ClientRequestURI', 'url_count'])
        with url_count.execute().collect() as results:
            for row in results:
                writer.writerow(row)
    
    print("URL Count Per Org written to url_count_result.csv")

    # Status Code Count per Org
    print("--- Status Code Count Per Org ---")
    status_count = table_env.sql_query("""
        SELECT x_org_uid, EdgeResponseStatus, COUNT(EdgeResponseStatus) AS status_count
        FROM Logs
        GROUP BY x_org_uid, EdgeResponseStatus
    """)
    
    with open('status_code_result.csv', 'w', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['x_org_uid', 'EdgeResponseStatus', 'status_count'])
        with status_count.execute().collect() as results:
            for row in results:
                writer.writerow(row)
    
    print("Status Code Count Per Org written to status_code_result.csv")


if __name__ == '__main__':
    main()
