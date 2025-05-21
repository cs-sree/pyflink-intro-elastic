import random
import json
import time
from kafka import KafkaConsumer
from pyflink.table import TableEnvironment, EnvironmentSettings
from pyflink.table.expressions import col
import csv
from threading import Timer
import os

# Kafka Configuration
KAFKA_TOPIC = 'test-logs'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
GROUP_ID = 'log_group'

# Initialize Kafka Consumer
consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id=GROUP_ID,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Initialize Flink Table Environment
env_settings = EnvironmentSettings.new_instance().in_batch_mode().build()
table_env = TableEnvironment.create(env_settings)

# Temporary file to hold incoming logs
temp_file = '/Users/harshavardhan.reddy/Documents/ETL/pyflink-intro-elastic/csv_output/tmp/kafka_aggregated_logs.json'
url_aggregated_file = '/Users/harshavardhan.reddy/Documents/ETL/pyflink-intro-elastic/csv_output/tmp/kafka_final_aggregated_logs.json'
status_aggregated_file = '/Users/harshavardhan.reddy/Documents/ETL/pyflink-intro-elastic/csv_output/tmp/kafka_final_aggregated_status_logs.json'
# Global buffer for incoming logs
log_buffer = []
BUFFER_SIZE = 100  # Flush to file every 100 messages
FLUSH_INTERVAL = 300  # 5 minutes in seconds

# Writing buffer to a file and triggering aggregation
def flush_and_aggregate():
    global log_buffer
    if log_buffer:
        print(f"[INFO] Flushing {len(log_buffer)} records to file...")

        # Append each JSON object as a new line
        with open(temp_file, 'a') as f:
            for record in log_buffer:
                f.write(json.dumps(record) + '\n')

        log_buffer = []

        print("[INFO] File flush complete. Registering table...")
        # Register the data as a Table (Overwrite each time)
        try:
            table_env.execute_sql(f'''
                DROP TEMPORARY TABLE IF EXISTS Logs
            ''')
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
            print("[INFO] Table registered successfully.")
        except Exception as e:
            print(f"[ERROR] Table creation failed: {e}")

        # URL Count per Org
        print("--- URL Count Per Org ---")
        url_count = table_env.sql_query("""
            SELECT x_org_uid, ClientRequestURI, COUNT(*) AS url_count
            FROM Logs
            GROUP BY x_org_uid, ClientRequestURI
        """)

        # Append to the final aggregation JSON file
        with open(url_aggregated_file, 'a') as file:
            with url_count.execute().collect() as results:
                for row in results:
                    file.write(json.dumps({
                        "x_org_uid": row[0],
                        "ClientRequestURI": row[1],
                        "url_count": row[2]
                    }) + '\n')
        print(f"[INFO] URL Count Per Org appended to {url_aggregated_file}")

        # Status Code Count per Org
        print("--- Status Code Count Per Org ---")
        status_count = table_env.sql_query("""
            SELECT x_org_uid, EdgeResponseStatus, COUNT(EdgeResponseStatus) AS status_count
            FROM Logs
            GROUP BY x_org_uid, EdgeResponseStatus
        """)

        # Append to the final aggregation JSON file
        with open(status_aggregated_file, 'a') as file:
            with status_count.execute().collect() as results:
                for row in results:
                    file.write(json.dumps({
                        "x_org_uid": row[0],
                        "EdgeResponseStatus": row[1],
                        "status_count": row[2]
                    }) + '\n')
        print(f"[INFO] Status Code Count Per Org appended to {status_aggregated_file}")

    else:
        print("[INFO] Buffer is empty, no flush needed.")

    # Restart the timer
    Timer(FLUSH_INTERVAL, flush_and_aggregate).start()

# Start the timer for periodic flushing and aggregation
flush_and_aggregate()

# Consuming messages from Kafka and buffering
print("[INFO] Starting to consume messages...")
for message in consumer:
    log_entry = message.value
    print(f"[INFO] Consumed log: {log_entry}")
    flattened_entry = log_entry.copy()
    flattened_entry['x_org_uid'] = log_entry['ResponseHeaders'].get('x-org-uid', '')
    log_buffer.append(flattened_entry)

    # Flush if buffer exceeds the defined size
    if len(log_buffer) >= BUFFER_SIZE:
        flush_and_aggregate()
