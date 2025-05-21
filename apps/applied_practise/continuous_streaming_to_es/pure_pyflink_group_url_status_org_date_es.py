import logging
import sys
from datetime import datetime
from elasticsearch import Elasticsearch, NotFoundError
from pyflink.common import RowKind
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, TableDescriptor, Schema
from pyflink.table.expressions import col, call
from pyflink.table.udf import udf

KAFKA_TOPIC = "test-logs"
KAFKA_BOOTSTRAP = "localhost:9092"
ES_HOST = "http://localhost:9200"
URL_INDEX = "url_counts"
STATUS_INDEX = "status_counts"


# --- UDF to extract date from ISO timestamp ---
def extract_date_udf():
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
    return extract_date


# --- Sink for URL-based counts ---
def upsert_url_to_es(row):
    if row.get_row_kind() not in (RowKind.INSERT, RowKind.UPDATE_AFTER):
        return

    org_id, url, count, date = row[0], row[1], row[2], row[3]
    safe_url = url.replace('/', '_').replace('?', '_').replace('&', '_')
    doc_id = f"{org_id}_{date}_{safe_url}"

    doc = {
        "org_id": org_id,
        "date": date,
        "url": url,
        "url_count": count
    }

    es = Elasticsearch(ES_HOST)
    try:
        es.get(index=URL_INDEX, id=doc_id)
        es.update(index=URL_INDEX, id=doc_id, body={"doc": {"url_count": count}})
        print(f"[URL] Updated {doc_id} → {count}")
    except NotFoundError:
        es.index(index=URL_INDEX, id=doc_id, document=doc)
        print(f"[URL] Inserted {doc_id} → {count}")


# --- Sink for Status Code-based counts ---
def upsert_status_to_es(row):
    if row.get_row_kind() not in (RowKind.INSERT, RowKind.UPDATE_AFTER):
        return

    org_id, status, count, date = row[0], row[1], row[2], row[3]
    doc_id = f"{org_id}_{date}_{status}"

    doc = {
        "org_id": org_id,
        "date": date,
        "status_code": status,
        "status_count": count
    }

    es = Elasticsearch(ES_HOST)
    try:
        es.get(index=STATUS_INDEX, id=doc_id)
        es.update(index=STATUS_INDEX, id=doc_id, body={"doc": {"status_count": count}})
        print(f"[STATUS] Updated {doc_id} → {count}")
    except NotFoundError:
        es.index(index=STATUS_INDEX, id=doc_id, document=doc)
        print(f"[STATUS] Inserted {doc_id} → {count}")


def hello_pyflink():
    # Environment setup
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    s_env.enable_checkpointing(5000)
    t_env = StreamTableEnvironment.create(s_env)

    # Register UDF
    t_env.create_temporary_function("extract_date", extract_date_udf())

    # Kafka source table definition
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

    # --- URL Grouping Table ---
    result_table_url = (
        t_env.from_path("kafka_table")
        .group_by(col("OrgUID"), col("DateStr"), col("ClientRequestURI"))
        .select(
            col("OrgUID"),
            col("ClientRequestURI").alias("URL"),
            col("ClientRequestURI").count.alias("URLCount"),
            col("DateStr")
        )
    )

    # --- Status Code Grouping Table ---
    result_table_status = (
        t_env.from_path("kafka_table")
        .group_by(col("OrgUID"), col("DateStr"), col("EdgeResponseStatus"))
        .select(
            col("OrgUID"),
            col("EdgeResponseStatus").alias("Status"),
            col("EdgeResponseStatus").count.alias("StatusCount"),
            col("DateStr")
        )
    )

    # Convert tables to changelog streams
    result_stream_url = t_env.to_changelog_stream(result_table_url)
    result_stream_status = t_env.to_changelog_stream(result_table_status)

    # Map each stream to Elasticsearch
    result_stream_url.map(upsert_url_to_es)
    result_stream_status.map(upsert_status_to_es)

    s_env.execute("Org URL + Status Count to Elasticsearch")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    hello_pyflink()


# Bottleneck	                                Description
# 1. Unbounded Flink State	                    Grouping by org + date + url/status can grow large and persist indefinitely.
# 2. High ES Upsert Load	                    Frequent GET + UPDATE per event causes I/O and latency issues.
# 3. No Retry Handling	                        Sink writes could fail without recovery.
# 4. Checkpoint Size & Recovery	                Large state = slow checkpointing and restart delays.
# 5. Per-event REST Overhead	                One REST call per record → high network/CPU usage.
# 6. No Backpressure Awareness	                Manual .map() sinks can’t signal when ES is slow.


# This code also aggregates URL and status counts from a Kafka topic and writes them to Elasticsearch.
# It uses a similar approach to the previous code but focuses on URL and status counts separately.
# The code is structured to handle the data in a more efficient way, reducing bottlenecks and improving performance.
# The application is designed to run continuously, processing data as it arrives in real-time.

