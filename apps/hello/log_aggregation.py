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
    t_env = StreamTableEnvironment.create(s_env)

    # Define a UDF to parse ISO 8601 timestamp strings
    @udf(result_type=DataTypes.TIMESTAMP_LTZ(3))
    def parse_iso_timestamp(iso_string):
        if iso_string is None:
            return None
        try:
            # Parse ISO 8601 string (e.g., '2025-05-08T15:09:40.271017')
            parsed_time = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
            # Truncate to millisecond precision (set microsecond to multiple of 1000)
            parsed_time = parsed_time.replace(microsecond=(parsed_time.microsecond // 1000) * 1000)
            # Ensure the datetime is timezone-aware (UTC)
            if parsed_time.tzinfo is None:
                parsed_time = parsed_time.replace(tzinfo=timezone.utc)
            return parsed_time
        except Exception as e:
            # Log error and return None (or handle as needed)
            logging.error(f"Failed to parse timestamp {iso_string}: {str(e)}")
            return None

    # Register the UDF
    t_env.create_temporary_function("parse_iso_timestamp", parse_iso_timestamp)

    # Kafka source table with event_time computed using the UDF
    t_env.create_temporary_table(
        "kafka_table",
        TableDescriptor.for_connector("kafka")
        .schema(
            Schema.new_builder()
            .column("log_format", DataTypes.STRING())  # ISO 8601 timestamp string
            .column("log_line", DataTypes.STRING())   # JSON log line
            .column_by_expression(
                "event_time",
                call("parse_iso_timestamp", col("log_format"))
            )  # Use call() to invoke UDF
            .watermark("event_time", "event_time - INTERVAL '5' SECOND")  # Watermark
            .build()
        )
        .format("json")
        .option("topic", "test-logs")
        .option("properties.bootstrap.servers", "localhost:9092")
        .option("properties.group.id", "pyflink-consumer-group")
        .option("scan.startup.mode", "earliest-offset")
        .build()
    )

    debug_table = t_env.from_path("kafka_table").select(col("log_format"), col("log_line"), col("event_time"))
    debug_table.execute().print()
    # Parse the `log_line` field, keeping event_time
    parsed_table = (
        t_env.from_path("kafka_table")
        .select(
            col("event_time"),
            col("log_line").json_value("$.attributes.organizationUid", DataTypes.STRING()).alias("organizationUid"),
            col("log_line").json_value("$.attributes.http.url", DataTypes.STRING()).alias("url")
        )
    )

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

    # Sink: Print the aggregated results
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

    result_table.execute_insert("print_sink").wait()

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    hello_pyflink()