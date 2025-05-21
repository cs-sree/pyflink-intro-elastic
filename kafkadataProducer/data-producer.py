import random
import json
import time
from kafka import KafkaProducer

# Kafka Configuration
KAFKA_TOPIC = 'test-logs'
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# Predefined values
client_ips = ['103.208.71.62', '103.208.71.80', '51.159.21.115', '185.36.188.92']
request_methods = ['GET', 'POST', 'HEAD']
request_uris = ['/v3/test/', '/v2/test/', '/v3/entries', '/v2/entries']
response_statuses = [200, 201, 404, 500]
x_org_uids = ['blt7558a3a6df62c471', 'blt7558a3a6df62c472', 'blt7558a3a6df62c473']

# Initialize Kafka Producer
producer = KafkaProducer(
    bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

def generate_log():
    log = {
        "ClientIP": random.choice(client_ips),
        "ClientRequestMethod": random.choice(request_methods),
        "ClientRequestURI": random.choice(request_uris),
        "EdgeEndTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "EdgeResponseBytes": random.randint(100, 1000),
        "EdgeResponseStatus": random.choice(response_statuses),
        "EdgeStartTimestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "RequestHeaders": {"x-user-agent": "curl"},
        "CacheCacheStatus": random.choice(['dynamic', 'hit', 'miss']),
        "CacheTieredFill": random.choice([True, False]),
        "ClientRequestBytes": random.randint(1000, 5000),
        "ClientRequestUserAgent": "curl/7.79.1",
        "EdgeColoCode": "BOM",
        "EdgeRequestHost": "prosociety.cscustomer.ml",
        "EdgeTimeToFirstByteMs": random.randint(100, 1000),
        "ResponseHeaders": {
            "x-project-uid": "63d78b05a41d525ceb1e8a41",
            "x-environment-uid": "63d78b05a41d525ceb1e8a45",
            "x-org-uid": random.choice(x_org_uids)
        }
    }
    return log

try:
    while True:
        log_entry = generate_log()
        producer.send(KAFKA_TOPIC, value=log_entry)
        print(f"Sent log: {log_entry}")
        time.sleep(random.uniform(0.1, 1.0))
except KeyboardInterrupt:
    print("Stopping producer...")
finally:
    producer.close()
