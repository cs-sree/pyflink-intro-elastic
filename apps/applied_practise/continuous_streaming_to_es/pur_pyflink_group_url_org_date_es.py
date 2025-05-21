import logging
import sys
from datetime import datetime
from elasticsearch import Elasticsearch, NotFoundError
from pyflink.common import RowKind
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, TableDescriptor, Schema
from pyflink.table.expressions import col, call
from pyflink.table.udf import udf

# Constants
KAFKA_TOPIC = "test-logs"
KAFKA_BOOTSTRAP = "localhost:9092"
ES_HOST = "http://localhost:9200"
ES_INDEX = "url_counts"


# Elasticsearch sink function
def upsert_to_elasticsearch(row):
    if row.get_row_kind() not in (RowKind.INSERT, RowKind.UPDATE_AFTER):
        return

    org_id = row[0]
    url = row[1]
    url_count = row[2]
    date_str = row[3]

    # Sanitize URL for doc_id
    safe_url = url.replace('/', '_').replace('?', '_').replace('&', '_')
    doc_id = f"{org_id}_{date_str}_{safe_url}"

    doc = {
        "org_id": org_id,
        "date": date_str,
        "url": url,
        "url_count": url_count
    }

    es = Elasticsearch(ES_HOST)
    try:
        es.get(index=ES_INDEX, id=doc_id)
        es.update(index=ES_INDEX, id=doc_id, body={"doc": {"url_count": url_count}})
        print(f"[ES] Updated {doc_id} → {url_count}")
    except NotFoundError:
        es.index(index=ES_INDEX, id=doc_id, document=doc)
        print(f"[ES] Inserted {doc_id} → {url_count}")


def hello_pyflink():
    # Set up environment
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    s_env.enable_checkpointing(5000)
    t_env = StreamTableEnvironment.create(s_env)

    # UDF to extract date from timestamp
    @udf(result_type=DataTypes.STRING())
    def extract_date(iso_string):
        if iso_string is None:
            return None
        try:
            dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
            return dt.date().isoformat()
        except Exception as e:
            logging.error(f"Failed to parse timestamp {iso_string}: {str(e)}")
            return None

    t_env.create_temporary_function("extract_date", extract_date)

    # Define Kafka source table
    t_env.create_temporary_table(
        "kafka_table",
        TableDescriptor.for_connector("kafka")
        .schema(
            Schema.new_builder()
            .column("EdgeEndTimestamp", DataTypes.STRING())
            .column("ClientRequestURI", DataTypes.STRING())
            .column("EdgeResponseStatus", DataTypes.INT())
            .column("ResponseHeaders", DataTypes.ROW([
                DataTypes.FIELD("x-org-uid", DataTypes.STRING())
            ]))
            .column_by_expression("OrgUID", col("ResponseHeaders").get("x-org-uid"))
            .column_by_expression("DateStr", call("extract_date", col("EdgeEndTimestamp")))
            .build()
        )
        .format("json")
        .option("topic", KAFKA_TOPIC)
        .option("properties.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("properties.group.id", "pyflink-consumer-group")
        .option("scan.startup.mode", "latest-offset")
        .build()
    )

    # Group by OrgUID + Date + URL
    result_table = (
        t_env.from_path("kafka_table")
        .group_by(col("OrgUID"), col("DateStr"), col("ClientRequestURI"))
        .select(
            col("OrgUID"),
            col("ClientRequestURI").alias("URL"),
            col("ClientRequestURI").count.alias("URLCount"),
            col("DateStr")
        )
    )

    # Convert to changelog stream
    result_stream = t_env.to_changelog_stream(result_table)

    result_stream.print()  # For debugging
    # Map each row and send to Elasticsearch
    result_stream.map(upsert_to_elasticsearch)

    # Execute the pipeline
    s_env.execute("Grouped URL Count to Elasticsearch")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    hello_pyflink()


# This code is a PyFlink application that reads data from a Kafka topic, processes it to count unique URLs per organization and date, and then writes the results to an Elasticsearch index.
# The application uses a user-defined function (UDF) to extract the date from a timestamp and handles both insertions and updates in Elasticsearch.
# The code is structured to be modular, with clear separation of concerns for reading from Kafka, processing the data, and writing to Elasticsearch.
# The Elasticsearch sink function is designed to handle both insert and update operations based on the existence of the document in the index.
# The application is set up to run in a streaming environment with checkpointing enabled for fault tolerance.
# The logging module is used for debugging and error handling, providing insights into the application's execution flow.
# The code is designed to be run as a standalone script, with the main function being executed when the script is run directly.
# The application is intended for use in a data pipeline where real-time processing of log data is required, such as monitoring web traffic or API usage.

# To see url aggrgeation and status code aggregation, go to file pure_pyflink_group_url_status_org_date_es.py
