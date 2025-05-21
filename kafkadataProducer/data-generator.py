from kafka import KafkaProducer
from time import sleep
import datetime
import json

# Configuration
LOG_FILE_PATH = './kafkadataProducer/2021-03-27.1.txt'  # Path to your log file
KAFKA_BROKER = 'localhost:9092'  # Update with your broker address
KAFKA_TOPIC = 'test-logs'  # Update with your Kafka topic

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=[KAFKA_BROKER],
    value_serializer=lambda v: v.encode('utf-8')
)

def send_logs_to_kafka():
    try:
        with open(LOG_FILE_PATH, 'r') as log_file:
            for log_line in log_file:
                # Remove any surrounding whitespace
                log_line = log_line.strip()

                if log_line:  # Ensure it's not an empty line
                    # Add timestamp to log
                    timestamped_log = json.dumps({
                        "log_format": datetime.datetime.now().isoformat(),
                        "log_line": log_line
                    })
                    # Send log to Kafka
                    producer.send(KAFKA_TOPIC, value=timestamped_log)
                    print(f"Sent log to Kafka: {timestamped_log}")

                    # Sleep for 60 seconds (1 minute) before sending the next log
                    sleep(60)

    except Exception as e:
        print(f"Error occurred: {e}")
    finally:
        producer.close()
        print("Kafka producer closed.")

# Run the log producer
send_logs_to_kafka()
