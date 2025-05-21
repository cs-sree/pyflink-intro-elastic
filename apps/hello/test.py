from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.elasticsearch import Elasticsearch7SinkBuilder

def hello_pyflink():
    env = StreamExecutionEnvironment.get_execution_environment()
    ELASTICSEARCH_SQL_CONNECTOR_BASE_PATH = "file:///Users/harshavardhan.reddy/Documents/ETL/pyflink-intro-elastic/venv/lib/python3.11/site-packages/pyflink/lib/flink-sql-connector-elasticsearch7-4.0.0-2.0.jar"
    env.add_jars(ELASTICSEARCH_SQL_CONNECTOR_BASE_PATH)
    data_stream = env.from_collection([
        {"name": "Alice", "age": 30},
        {"name": "Bob", "age": 25},
    ])

    print(f'printing jars {env}')
    # ✅ Just pass strings, not HttpHost objects!
    es_sink = Elasticsearch7SinkBuilder() \
        .set_hosts(["http://localhost:9200"]) \
        .set_index("my-index") \
        .set_document_type("_doc") \
        .build()

    data_stream.add_sink(es_sink)
    env.execute("Hello PyFlink to Elasticsearch")

if __name__ == '__main__':
    
    hello_pyflink()