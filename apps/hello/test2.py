from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.elasticsearch import Elasticsearch7SinkBuilder
from pyflink.common import Types
import json

def create_emitter():
    def emit(record):
        return json.dumps({
            "index": "my-index",
            "id": record["name"],
            "source": record
        })
    return emit

def hello_pyflink():
    # Initialize Flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Add Elasticsearch connector JAR
    ELASTICSEARCH_SQL_CONNECTOR_BASE_PATH = "file:///opt/flink/lib/flink-sql-connector-elasticsearch7-4.0.0-2.0.jar"
    env.add_jars(ELASTICSEARCH_SQL_CONNECTOR_BASE_PATH)

    # Sample data stream
    data_stream = env.from_collection(
        [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ],
        type_info=Types.ROW([Types.STRING(), Types.INT()])
    )

    # Configure Elasticsearch sink
    emitter = create_emitter()
    sink = (Elasticsearch7SinkBuilder()
            .set_hosts(["http://localhost:9200"])
            .set_emitter(emitter)
            .build())

    # Add sink to data stream
    data_stream.add_sink(sink)

    # Execute the job
    env.execute("Hello PyFlink to Elasticsearch")

if __name__ == "__main__":
    hello_pyflink()