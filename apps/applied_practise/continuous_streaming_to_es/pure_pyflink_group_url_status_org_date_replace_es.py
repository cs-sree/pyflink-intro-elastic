import logging
import sys
from datetime import datetime
from elasticsearch import Elasticsearch, ElasticsearchException
from pyflink.common import RowKind, Duration
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, TableDescriptor, Schema
from pyflink.table.expressions import col, call
from pyflink.table.udf import udf

KAFKA_TOPIC = "test-logs"
KAFKA_BOOTSTRAP = "localhost:9092"
ES_HOST = "http://localhost:9200"
URL_INDEX = "url_counts"
STATUS_INDEX = "status_counts"
ORG_INDEX = "org_latest"

# Batch buffers
url_buffer = []
status_buffer = []
BATCH_SIZE = 20

es = Elasticsearch(ES_HOST)

# Flush buffer to ES in bulk
def flush_to_es(index, buffer, id_fn):
    if not buffer:
        return
    actions = []
    for doc in buffer:
        doc_id = id_fn(doc)
        actions.append({
            "update": {
                "_index": index,
                "_id": doc_id
            }
        })
        actions.append({
            "doc": doc, "doc_as_upsert": True })
    try:
        es.bulk(body=actions)
        print(f"[ES] Flushed {len(buffer)} to {index}")
    except ElasticsearchException as e:
        logging.error(f"[ES] Bulk flush failed: {e}")
    buffer.clear()

# UDF to extract date
@udf(result_type=DataTypes.STRING())
def extract_date(iso_string):
    if iso_string is None:
        return None
    try:
        dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
        return dt.date().isoformat()
    except Exception as e:
        logging.error(f"Timestamp parse failed: {e}")
        return None

def upsert_url(row):
    if row.get_row_kind() not in (RowKind.INSERT, RowKind.UPDATE_AFTER):
        return
    org_id, url, count, date = row[0], row[1], row[2], row[3]
    safe_url = url.replace('/', '_').replace('?', '_').replace('&', '_')
    doc = {
        "org_id": org_id,
        "date": date,
        "url": url,
        "url_count": count
    }
    url_buffer.append(doc)
    if len(url_buffer) >= BATCH_SIZE:
        flush_to_es(URL_INDEX, url_buffer, lambda d: f"{d['org_id']}_{d['date']}_{d['url'].replace('/', '_')}")

def upsert_status(row):
    if row.get_row_kind() not in (RowKind.INSERT, RowKind.UPDATE_AFTER):
        return
    org_id, status, count, date = row[0], row[1], row[2], row[3]
    doc = {
        "org_id": org_id,
        "date": date,
        "status_code": status,
        "status_count": count
    }
    status_buffer.append(doc)
    if len(status_buffer) >= BATCH_SIZE:
        flush_to_es(STATUS_INDEX, status_buffer, lambda d: f"{d['org_id']}_{d['date']}_{d['status_code']}")

def replace_latest_org(row):
    if row.get_row_kind() not in (RowKind.INSERT, RowKind.UPDATE_AFTER):
        return
    org_id, date, status = row[0], row[1], row[2]
    doc_id = f"{org_id}"
    doc = {
        "org_id": org_id,
        "latest_date": date,
        "last_status": status
    }
    try:
        es.index(index=ORG_INDEX, id=doc_id, document=doc)
        print(f"[ORG] Replaced {doc_id} → {doc}")
    except ElasticsearchException as e:
        logging.error(f"[ORG] Replace failed for {doc_id}: {e}")

def hello_pyflink():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    s_env.enable_checkpointing(10000)
    t_env = StreamTableEnvironment.create(s_env)

    # State retention (TTL)
    t_env.get_config().set_idle_state_retention_time(Duration.of_minutes(10), Duration.of_hours(1))

    t_env.create_temporary_function("extract_date", extract_date)

    # Kafka Source Table
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

    table_url = (
        t_env.from_path("kafka_table")
        .group_by(col("OrgUID"), col("DateStr"), col("ClientRequestURI"))
        .select(
            col("OrgUID"),
            col("ClientRequestURI").alias("URL"),
            col("ClientRequestURI").count.alias("URLCount"),
            col("DateStr")
        )
    )

    table_status = (
        t_env.from_path("kafka_table")
        .group_by(col("OrgUID"), col("DateStr"), col("EdgeResponseStatus"))
        .select(
            col("OrgUID"),
            col("EdgeResponseStatus").alias("Status"),
            col("EdgeResponseStatus").count.alias("StatusCount"),
            col("DateStr")
        )
    )

    table_replace = (
        t_env.from_path("kafka_table")
        .select(
            col("OrgUID"),
            col("DateStr"),
            col("EdgeResponseStatus")
        )
    )

    stream_url = t_env.to_changelog_stream(table_url)
    stream_status = t_env.to_changelog_stream(table_status)
    stream_replace = t_env.to_changelog_stream(table_replace)

    stream_url.map(upsert_url)
    stream_status.map(upsert_status)
    stream_replace.map(replace_latest_org)

    s_env.execute("Enhanced Flink to ES with Bottleneck Fixes and Replace Logic")

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    hello_pyflink()


# This code has simialr  codes what we have in the previous code snippets.
# This code also has a logic that replaces the latest org data in ES which is useful in the case of where we already receives the aggaregated data .
