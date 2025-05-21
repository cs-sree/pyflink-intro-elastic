import logging
import sys
from datetime import datetime, timezone
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, TableDescriptor, Schema
from pyflink.table.window import Tumble
from pyflink.table.expressions import col, lit, call
from pyflink.table.udf import udf

def hello_pyflink():
    # Environment setup
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)  # Set explicit parallelism for local execution
    t_env = StreamTableEnvironment.create(s_env)
    logging.debug("Flink environment initialized with parallelism 1")

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
        .option("scan.startup.mode", "earliest-offset")
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

    # Define a tumbling window of 10 seconds
    result_table = (
        parsed_table
        .window(Tumble.over(lit(10).minutes).on(col("event_time")).alias("w"))
        .group_by(col("organizationUid"), col("w"))
        .select(
            col("organizationUid"),
            col("url").count.alias("url_count"),
            col("w").start.alias("window_start"),
            col("w").end.alias("window_end")
        )
    )
    logging.debug("Result table defined")

    # Sink: Print to console
    t_env.create_temporary_table(
        "print_sink",
        TableDescriptor.for_connector("print")
        .schema(
            Schema.new_builder()
            .column("organizationUid", DataTypes.STRING())
            .column("url_count", DataTypes.BIGINT())
            .column("window_start", DataTypes.TIMESTAMP_LTZ(3))
            .column("window_end", DataTypes.TIMESTAMP_LTZ(3))
            .build()
        )
        .build()
    )
    logging.debug("Print sink created")

    # Execute the pipeline
    result_table.execute_insert("print_sink").wait()
    logging.debug("Pipeline executed")

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
    hello_pyflink()