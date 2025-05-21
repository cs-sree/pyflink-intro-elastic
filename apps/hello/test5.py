from pyflink.datastream import StreamExecutionEnvironment
from pyflink.datastream.connectors.elasticsearch import Elasticsearch7SinkBuilder
from pyflink.common import Types
import json

def hello_pyflink():
    # Initialize Flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Add Elasticsearch connector JAR
    ELASTICSEARCH_SQL_CONNECTOR_BASE_PATH = "file:///opt/flink/lib/flink-connector-elasticsearch7-3.1.0-1.19.jar"
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
    sink = (Elasticsearch7SinkBuilder()
            .set_hosts(["http://localhost:9200"])
            .set_emitter(
                lambda record: json.dumps({
                    "index": "my-index",
                    "id": record["name"],
                    "source": record
                })
            )
            .build())

    # Add sink to data stream
    data_stream.add_sink(sink)

    # Execute the job
    env.execute("Hello PyFlink to Elasticsearch")

if __name__ == "__main__":
    hello_pyflink()