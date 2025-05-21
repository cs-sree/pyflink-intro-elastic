from pyflink.datastream import StreamExecutionEnvironment
from pyflink.common import Types
from pyflink.datastream.functions import SinkFunction
from elasticsearch import Elasticsearch
import json

class ElasticsearchSink(SinkFunction):
    def __init__(self, hosts):
        self.es = Elasticsearch(hosts=hosts)
        self.index_name = "my-index"

    def invoke(self, value, context):
        # Serialize record to JSON
        doc = {
            "name": value["name"],
            "age": value["age"]
        }
        # Index the document in Elasticsearch
        self.es.index(index=self.index_name, id=value["name"], body=doc)

def hello_pyflink():
    # Initialize Flink environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Sample data stream
    data_stream = env.from_collection(
        [
            {"name": "Alice", "age": 30},
            {"name": "Bob", "age": 25},
        ],
        type_info=Types.ROW([Types.STRING(), Types.INT()])
    )

    # Add custom Elasticsearch sink
    data_stream.add_sink(ElasticsearchSink(hosts=["http://localhost:9200"]))

    # Execute the job
    env.execute("Hello PyFlink to Elasticsearch")

if __name__ == "__main__":
    hello_pyflink()

   /Users/harshavardhan.reddy/Downloads/elasticsearch-7.17.0.jar

/Users/harshavardhan.reddy/Downloads/flink-connector-elasticsearch-base-1.16.0.jar