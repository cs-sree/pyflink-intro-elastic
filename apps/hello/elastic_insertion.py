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
from pyflink.datastream.connectors.elasticsearch import Elasticsearch7SinkBuilder, ElasticsearchEmitter
# from pyflink.datastream.connectors.elasticsearch import FlinkElasticsearch7SinkBuilder, ElasticsearchEmitter

def hello_pyflink():
    # Environment setup
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)  # Set explicit parallelism for local execution
    s_env.enable_checkpointing(5000)  # Checkpoint every 5 seconds
    s_env.get_checkpoint_config().set_checkpoint_timeout(20000)  # 20-second timeout
    s_env.get_checkpoint_config().set_checkpoint_storage_dir("file:///tmp/pyflink_checkpoints")
    
    # Add Elasticsearch connector JAR
    # ELASTICSEARCH_SQL_CONNECTOR_PATH = "file:///Users/harshavardhan.reddy/flinkProject/examples/pyflink-intro/venv/lib/python3.11/site-packages/pyflink/lib/flink-connector-elasticsearch7-3.0.1-1.17.jar"
    ELASTIC_FLINK_SQL_CONNECTOR_PATH = "file:///Users/harshavardhan.reddy/flinkProject/examples/pyflink-intro/venv/lib/python3.11/site-packages/pyflink/lib/flink-sql-connector-kafka-3.2.0-1.19.jar"
    ELASTICSEARCH_SQL_CONNECTOR_PATH = "file:///Users/harshavardhan.reddy/Documents/ETL/flink-1.20.0/lib/flink-connector-elasticsearch7-3.1.0.1.20.jar"
    ELASTICSEARCH_SQL_CONNECTOR_FLINK_PATH = "file:///Users/harshavardhan.reddy/Documents/ETL/flink-1.20.0/lib/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar"
    ELASTICSEARCH_TABLE_CONNECTOR_PATH = "file:///Users/harshavardhan.reddy/flinkProject/examples/pyflink-intro/venv/lib/python3.11/site-packages/pyflink/lib/flink-table-planner_2.12-1.20.0.jar"
    ELASTICSEARCH_CONNECTOR_BASE_PATH="file:///Users/harshavardhan.reddy/flinkProject/examples/pyflink-intro/venv/lib/python3.11/site-packages/pyflink/lib/flink-connector-base-1.20.0.jar"
    ELASTICSEARCH_SQL_CONNECTOR_BASE_PATH = "file:///Users/harshavardhan.reddy/flinkProject/examples/pyflink-intro/venv/lib/python3.11/site-packages/pyflink/lib/flink-connector-elasticsearch-base_2.12-1.14.6.jar"
    s_env.add_jars(ELASTICSEARCH_SQL_CONNECTOR_PATH, ELASTICSEARCH_SQL_CONNECTOR_BASE_PATH,ELASTICSEARCH_CONNECTOR_BASE_PATH,ELASTICSEARCH_TABLE_CONNECTOR_PATH, ELASTICSEARCH_SQL_CONNECTOR_FLINK_PATH)
    
    t_env = StreamTableEnvironment.create(s_env)
    # t_env.get_config().set("pipeline.jars",ELASTICSEARCH_SQL_CONNECTOR_PATH)
    # t_env.get_config().set("pipeline.jars",ELASTICSEARCH_TABLE_CONNECTOR_PATH)
    # t_env.get_config().set("pipeline.jars",ELASTICSEARCH_CONNECTOR_BASE_PATH)
    # t_env.get_config().set("pipeline.jars",ELASTICSEARCH_SQL_CONNECTOR_BASE_PATH)
    # t_env.get_config().set("pipeline.jars",ELASTIC_FLINK_SQL_CONNECTOR_PATH)
    jars = [
        "file:///Users/harshavardhan.reddy/Documents/ETL/flink-1.20.0/lib/kafka-clients-3.7.1.jar",
        "file:///Users/harshavardhan.reddy/Documents/ETL/flink-1.20.0/lib/flink-connector-kafka-3.2.0-1.20.jar",
        "file:///Users/harshavardhan.reddy/Documents/ETL/flink-1.20.0/lib/flink-connector-elasticsearch7-3.1.0.1.20.jar",
        "file:///Users/harshavardhan.reddy/Documents/ETL/flink-1.20.0/lib/flink-connector-elasticsearch-base-3.0.1-1.17.jar",
        "file:///Users/harshavardhan.reddy/Documents/ETL/flink-1.20.0/lib/httpclient-4.5.13.jar",
        "file:///Users/harshavardhan.reddy/Documents/ETL/flink-1.20.0/lib/httpcore-4.4.13.jar",
        "file:///Users/harshavardhan.reddy/Documents/ETL/flink-1.20.0/lib/flink-table-planner_2.12-1.20.0.jar"
    ]
    # t_env.get_config().set("pipeline.jars", ";".join(jars))
    logging.debug("Flink environment initialized with parallelism 1 and checkpointing")

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
    logging.debug("UDF registered")

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
    logging.debug("Kafka source table created")

    # Parse the `log_line` field
    parsed_table = (
        t_env.from_path("kafka_table")
        .select(
            col("event_time"),
            col("log_line").json_value("$.attributes.organizationUid", DataTypes.STRING()).alias("organizationUid"),
            col("log_line").json_value("$.attributes.http.url", DataTypes.STRING()).alias("url")
        )
    )
    logging.debug("Parsed table defined")

    # Debug: Print parsed fields
    # parsed_table.execute().print()
    logging.debug("Parsed table execution triggered")

    # Filter non-null records (bypassed as per request)
    filtered_table = parsed_table
    logging.debug("Filtered table defined (bypassed)")

    # Define a tumbling window of 5 minutes
    result_table = (
        filtered_table
        .window(Tumble.over(lit(5).minutes).on(col("event_time")).alias("w"))
        .group_by(col("organizationUid"), col("w"))
        .select(
            col("organizationUid"),
            col("url").count.alias("url_count"),
            col("w").start.alias("window_start"),
            col("w").end.alias("window_end")
        )
    )
    logging.debug("Result table defined")

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

    result_stream.print()  # Print the result stream for debugging

    # Elasticsearch sink with update script
    es_sink = (
        Elasticsearch7SinkBuilder()
       .set_hosts('localhost:9200')
        .set_emitter(
            lambda row, index: {
                "index": "url_counts",
                "id": f"{row['organizationUid']}_{row['window_start'].isoformat()}",
                "op_type": "update",
                "doc": {
                    "script": {
                        "source": "ctx._source.url_count += params.url_count; ctx._source.organizationUid = params.organizationUid; ctx._source.window_start = params.window_start; ctx._source.window_end = params.window_end",
                        "lang": "painless",
                        "params": {
                            "url_count": row["url_count"],
                            "organizationUid": row["organizationUid"],
                            "window_start": row["window_start"].isoformat(),
                            "window_end": row["window_end"].isoformat()
                        }
                    },
                    "upsert": {
                        "organizationUid": row["organizationUid"],
                        "url_count": row["url_count"],
                        "window_start": row["window_start"].isoformat(),
                        "window_end": row["window_end"].isoformat()
                    }
                }
            }
        )
        .set_bulk_flush_max_actions(1000)
        .set_bulk_flush_interval(5000)
        .set_connection_request_timeout(30000)
        .set_connection_timeout(15000)
        .set_socket_timeout(60000)
        .build()
    )

    # Add sink to DataStream
    result_stream.sink_to(es_sink).name("es7 sink")
    logging.debug("Elasticsearch sink added")

    # Execute the pipeline
    s_env.execute("PyFlink Elasticsearch Job")
    logging.debug("Pipeline executed")

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
    hello_pyflink()