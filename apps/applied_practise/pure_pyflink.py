# This code is used to read from kafka and write to file sink using 
# pyflink table api using tumbling window

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
import os


def hello_pyflink():
    # Environment setup
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    s_env.enable_checkpointing(5000)
    s_env.get_checkpoint_config().set_checkpoint_timeout(20000)

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
            .column("EdgeEndTimestamp", DataTypes.STRING())
            .column("ClientRequestURI", DataTypes.STRING())
            .column("EdgeResponseStatus", DataTypes.INT())
            .column("ResponseHeaders", DataTypes.ROW([DataTypes.FIELD("x-org-uid", DataTypes.STRING())]))
            .column_by_expression("x_org_uid", col("ResponseHeaders").get("x-org-uid"))
            .column_by_expression("EventTime", call("parse_iso_timestamp", col("EdgeEndTimestamp")))
            .watermark("EventTime", "EventTime - INTERVAL '1' SECOND")
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
            col("EventTime"),
            col("x_org_uid").alias("OrgUID"),
            col("ClientRequestURI").alias("URL")
        )
    )

    print("Parsed Table:")
    # parsed_table.execute().print()

    # Define a tumbling window of 5 minutes
    result_table = (
        parsed_table
        .window(Tumble.over(lit(5).minutes).on(col("EventTime")).alias("w"))
        .group_by(col("OrgUID"), col("w"))
        .select(
            col("OrgUID"),
            col("URL").count.alias("URLCount"),
            col("w").start.alias("WindowStart"),
            col("w").end.alias("WindowEnd")
        )
    )

    print("Result Table:")
    # result_table.execute().print()

    # Register the result table as a temporary view
    t_env.create_temporary_view("result_view", result_table)

    # ========================
    # File Sink Configuration
    # ========================
    output_path = '/Users/harshavardhan.reddy/Documents/ETL/pyflink-intro-elastic/csv_output/grouped_logs'
    os.makedirs(output_path, exist_ok=True)

    # Define Filesystem Sink
    t_env.execute_sql("""
    CREATE TABLE file_sink (
        OrgUID STRING,
        URLCount BIGINT,
        WindowStart TIMESTAMP(3),
        WindowEnd TIMESTAMP(3)
    ) WITH (
        'connector' = 'filesystem',
        'path' = 'file:///Users/harshavardhan.reddy/Documents/ETL/pyflink-intro-elastic/csv_output/grouped_logs',
        'format' = 'csv'
    )
    """)

    # Insert grouped data into file
    t_env.execute_sql("""
    INSERT INTO file_sink
    SELECT
        OrgUID,
        URLCount,
        WindowStart,
        WindowEnd
    FROM result_view
    """).wait()
    print("Data written to file sink.")

    # Execute the pipeline
    # s_env.execute("PyFlink File Sink Job")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG, format="%(asctime)s - %(levelname)s - %(message)s")
    hello_pyflink()
