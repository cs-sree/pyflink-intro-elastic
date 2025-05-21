import logging
import sys
from datetime import datetime, timezone
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, TableDescriptor, Schema
from pyflink.table.window import Tumble
from pyflink.table.expressions import col, lit, call
from pyflink.table.udf import udf
from pyflink.common import Row
from pyflink.common.typeinfo import Types
import requests
import json
from pyflink.datastream.functions import MapFunction


# class ElasticsearchSink:
#     def __init__(self, index_name, es_host="http://172.17.0.3:9200"):
#         self.index_name = index_name
#         self.es_host = es_host

#     def invoke(self, value, context):
#         url = f"{self.es_host}/{self.index_name}/_doc/"
#         headers = {'Content-Type': 'application/json'}
#         response = requests.post(url, headers=headers, data=json.dumps(value))
#         if response.status_code not in (200, 201):
#             print(f"❌ Failed to insert document: {response.text}")
#         else:
#             print(f"✅ Successfully inserted document: {value}")


class ElasticsearchMap(MapFunction):
    def __init__(self, index_name, es_host="http://localhost:9200"):
        self.index_name = index_name
        self.es_host = es_host

    def map(self, value):
        document = {
            "organizationUid": value[0],
            "url_count": value[1],
            "window_start": value[2].isoformat(),
            "window_end": value[3].isoformat()
        }
        url = f"{self.es_host}/{self.index_name}/_doc/"
        headers = {'Content-Type': 'application/json'}
        response = requests.post(url, headers=headers, data=json.dumps(document))
        if response.status_code not in (200, 201):
            print(f"❌ Failed to insert document: {response.text}")
        else:
            print(f"✅ Successfully inserted document: {document}")
        return value



def hello_pyflink():
    # Environment setup
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    s_env.enable_checkpointing(5000)
    s_env.get_checkpoint_config().set_checkpoint_timeout(20000)
    # s_env.get_checkpoint_config().set_checkpoint_storage_dir("file:///tmp/pyflink_checkpoints")

    t_env = StreamTableEnvironment.create(s_env)

    # Define a UDF to parse ISO 8601 timestamp strings
    @udf(result_type=DataTypes.TIMESTAMP_LTZ(3))
    def parse_iso_timestamp(iso_string):
        if iso_string is None:
            return None
        try:
            parsed_time = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
            parsed_time = parsed_time.replace(microsecond=(parsed_time.microsecond // 1000) * 1000)
            if parsed_time.tzinfo is None:
                parsed_time = parsed_time.replace(tzinfo=timezone.utc)
            return parsed_time
        except Exception as e:
            logging.error(f"Failed to parse timestamp {iso_string}: {str(e)}")
            return None

    # Register the UDF
    t_env.create_temporary_function("parse_iso_timestamp", parse_iso_timestamp)

    # Kafka source table
    t_env.create_temporary_table(
        "kafka_table",
        TableDescriptor.for_connector("kafka")
        .schema(
            Schema.new_builder()
            .column("log_format", DataTypes.STRING())
            .column("log_line", DataTypes.STRING())
            .column_by_expression(
                "event_time",
                call("parse_iso_timestamp", col("log_format"))
            )
            .watermark("event_time", "event_time - INTERVAL '1' SECOND")
            .build()
        )
        .format("json")
        .option("topic", "test-logs")
        .option("properties.bootstrap.servers", "localhost:9092")
        .option("properties.group.id", "pyflink-consumer-group")
        .option("scan.startup.mode", "latest-offset")
        .build()
    )

    

    # Parse the `log_line` field
    parsed_table = (
        t_env.from_path("kafka_table")
        .select(
            col("event_time"),
            col("log_line").json_value("$.attributes.organizationUid", DataTypes.STRING()).alias("organizationUid"),
            col("log_line").json_value("$.attributes.http.url", DataTypes.STRING()).alias("url")
        )
    )
    parsed_table.execute().print()
    # Define a tumbling window of 5 minutes
    result_table = (
        parsed_table
        .window(Tumble.over(lit(5).minutes).on(col("event_time")).alias("w"))
        .group_by(col("organizationUid"), col("w"))
        .select(
            col("organizationUid"),
            col("url").count.alias("url_count"),
            col("w").start.alias("window_start"),
            col("w").end.alias("window_end")
        )
    )

    # Convert Table to DataStream
    result_stream = t_env.to_append_stream(
        result_table,
        Types.ROW([
            Types.STRING(),
            Types.LONG(),
            Types.SQL_TIMESTAMP(),
            Types.SQL_TIMESTAMP()
        ])
    )

    result_stream.print()

    # # Add custom Elasticsearch Sink
    # es_sink = ElasticsearchSink(index_name="url_counts")
    # result_stream.add_sink(es_sink.invoke)
    result_stream.map(ElasticsearchMap(index_name="url_counts"))

    # Execute the pipeline
    s_env.execute("PyFlink Elasticsearch Job")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
    hello_pyflink()
