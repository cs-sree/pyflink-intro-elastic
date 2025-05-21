import logging
import sys
from datetime import datetime, timezone
from elasticsearch import Elasticsearch, NotFoundError
from pyflink.common import RowKind
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment, DataTypes, TableDescriptor, Schema
from pyflink.table.expressions import col, call
from pyflink.table.udf import udf

KAFKA_TOPIC = "test-logs"
KAFKA_BOOTSTRAP = "localhost:9092"
ES_HOST = "http://localhost:9200"
ES_INDEX = "url_counts"

# Elasticsearch upsert function
def upsert_to_elasticsearch(row):
    if row.get_row_kind() not in (RowKind.INSERT, RowKind.UPDATE_AFTER):
        return

    org_id = row[0]
    url_count = row[1]
    date_str = row[2]

    doc_id = f"{org_id}_{date_str}"
    doc = {
        "org_id": org_id,
        "date": date_str,
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
    # Environments
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

    # Kafka Table Source
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

    # Aggregation Table
    result_table = (
        t_env.from_path("kafka_table")
        .group_by(col("OrgUID"), col("DateStr"))
        .select(
            col("OrgUID"),
            col("ClientRequestURI").count.alias("URLCount"),
            col("DateStr")
        )
    )

    # Convert to changelog stream
    result_stream = t_env.to_changelog_stream(result_table)

    # Apply Elasticsearch sink via map
    result_stream.map(upsert_to_elasticsearch)

    s_env.execute("TableAPI to Elasticsearch via DataStream")


if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    hello_pyflink()


# This code is a PyFlink application that reads data from a Kafka topic, processes it, and writes the results to an Elasticsearch index.
# It uses a UDF to extract the date from a timestamp and performs aggregations on the data.
# The results are then upserted into Elasticsearch, either inserting new documents or updating existing ones.
# The code is structured to handle streaming data and uses the PyFlink API for data processing.
# This code is implemented to update the URL count in Elasticsearch based on the incoming data stream grouping org_id and date.
# The application is designed to run continuously, processing data as it arrives in real-time.

# If we need grouping based on org_id, url, and date refer the file pur_pyflink_group_url_org_date_es.py