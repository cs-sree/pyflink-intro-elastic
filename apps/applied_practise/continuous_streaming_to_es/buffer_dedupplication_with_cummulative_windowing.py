import logging
import sys
import threading
import time
from datetime import datetime
from elasticsearch import Elasticsearch
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

# Buffers as key-value maps to drop older duplicate updates
url_buffer = {}
status_buffer = {}
BATCH_SIZE = 20

# removed global Elasticsearch client

# Flush buffer to ES in bulk
def flush_to_es(index, buffer, id_fn):
    if not buffer:
        return
    from elasticsearch import Elasticsearch
    es = Elasticsearch(ES_HOST)
    actions = []
    for doc_id, doc in buffer.items():
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
    except Exception as e:
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
    org_id, url, count, window_start = row[0], row[1], row[2], row[3]
    date_str = str(window_start).replace(':', '-').replace(' ', '_')
    safe_url = url.replace('/', '_').replace('?', '_').replace('&', '_')
    doc_id = f"{org_id}_{date_str}_{safe_url}"
    doc = {
        "org_id": org_id,
        "window_start": str(window_start),
        "url": url,
        "url_count": count
    }
    url_buffer[doc_id] = doc
    if len(url_buffer) >= BATCH_SIZE:
            flush_to_es(URL_INDEX, url_buffer, lambda d: d)
    else:
        print(f"[URL] Current unique buffer size: {len(url_buffer)} / {BATCH_SIZE}")

def upsert_status(row):
    if row.get_row_kind() not in (RowKind.INSERT, RowKind.UPDATE_AFTER):
        return
    org_id, status, count, window_start = row[0], row[1], row[2], row[3]
    date_str = str(window_start).replace(':', '-').replace(' ', '_')
    doc_id = f"{org_id}_{date_str}_{status}"
    doc = {
        "org_id": org_id,
        "window_start": str(window_start),
        "status_code": status,
        "status_count": count
    }
    status_buffer[doc_id] = doc
    if len(status_buffer) >= BATCH_SIZE:
            flush_to_es(STATUS_INDEX, status_buffer, lambda d: d)
    else:
        print(f"[STATUS] Current unique buffer size: {len(status_buffer)} / {BATCH_SIZE}")

def replace_latest_org(row):
    if row.get_row_kind() not in (RowKind.INSERT, RowKind.UPDATE_AFTER):
        return
    from elasticsearch import Elasticsearch
    es = Elasticsearch(ES_HOST)
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
    except Exception as e:
        logging.error(f"[ORG] Replace failed for {doc_id}: {e}")

def start_periodic_flush():
    def flush_loop():
        while True:
            time.sleep(10)  # Flush interval in seconds
            flush_to_es(URL_INDEX, url_buffer, lambda d: d)
            flush_to_es(STATUS_INDEX, status_buffer, lambda d: d)
    thread = threading.Thread(target=flush_loop, daemon=True)
    thread.start()

def hello_pyflink():
    s_env = StreamExecutionEnvironment.get_execution_environment()
    s_env.set_parallelism(1)
    s_env.enable_checkpointing(10000)
    t_env = StreamTableEnvironment.create(s_env)

    # State retention (TTL)
    t_env.get_config().get_configuration().set_string("table.exec.state.ttl", "3600 s")

    start_periodic_flush()

    from pyflink.table.window import Slide
    from pyflink.table.expressions import lit

    t_env.create_temporary_function("extract_date", extract_date)

    @udf(result_type=DataTypes.TIMESTAMP_LTZ(3))
    def parse_iso_timestamp(iso_string):
        if iso_string is None:
            return None
        try:
            dt = datetime.fromisoformat(iso_string.replace('Z', '+00:00'))
            return dt
        except Exception as e:
            logging.error(f"Failed to parse timestamp: {e}")
            return None

    t_env.create_temporary_function("parse_iso_timestamp", parse_iso_timestamp)

    # Kafka Source Table
    t_env.create_temporary_table(
        "kafka_table",
        TableDescriptor.for_connector("kafka")
        .schema(
            Schema.new_builder()
            .column("EdgeEndTimestamp", DataTypes.STRING())
            .column_by_expression("EventTime", call("parse_iso_timestamp", col("EdgeEndTimestamp")))
            .watermark("EventTime", "EventTime - INTERVAL '1' SECOND")
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
        .window(
            Slide.over(lit(60).minutes)
            .every(lit(10).minutes)
            .on(col("EventTime"))
            .alias("w")
        )
        .group_by(col("OrgUID"), col("ClientRequestURI"), col("w"))
        .select(
            col("OrgUID"),
            col("ClientRequestURI").alias("URL"),
            col("ClientRequestURI").count.alias("URLCount"),
            col("w").start.alias("WindowStart")
        )
    )

    table_status = (
        t_env.from_path("kafka_table")
        .window(
            Slide.over(lit(60).minutes)
            .every(lit(10).minutes)
            .on(col("EventTime"))
            .alias("w")
        )
        .group_by(col("OrgUID"), col("EdgeResponseStatus"), col("w"))
        .select(
            col("OrgUID"),
            col("EdgeResponseStatus").alias("Status"),
            col("EdgeResponseStatus").count.alias("StatusCount"),
            col("w").start.alias("WindowStart")
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

    # removed SinkFunction import because we use .map() instead

    stream_url.map(upsert_url)
    stream_status.map(upsert_status)
    stream_replace.map(replace_latest_org)

    # Final flush before exit (useful for testing)
    flush_to_es(URL_INDEX, url_buffer, lambda d: d)
    flush_to_es(STATUS_INDEX, status_buffer, lambda d: d)

    s_env.execute("Enhanced Flink to ES with Bottleneck Fixes and Replace Logic")

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    hello_pyflink()
