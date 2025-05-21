# PyFlink Kafka to Elasticsearch Applications

This repository contains a set of PyFlink applications designed for real-time streaming data processing. Each application reads data from a Kafka topic, performs various aggregations, and writes the results to an Elasticsearch index.

---

## 📁 Files Overview

### 1. `pur_pyflink_group_url_org_date_es.py`

**Description:**
- Reads data from a Kafka topic and counts **unique URLs per organization and date**.
- Utilizes a **User Defined Function (UDF)** to extract the date from timestamps.
- Writes results to Elasticsearch, handling both **inserts and updates**.
- Structured for modularity with clear separation of:
  - Kafka source
  - Data processing logic
  - Elasticsearch sink
- Uses the **streaming execution environment** with **checkpointing** for fault tolerance.
- Leverages the `logging` module for debugging and execution flow tracking.
- Can be run as a **standalone script**.
- Suitable for real-time data pipelines such as **web traffic or API usage monitoring**.
- 📌 For URL and status code aggregation, see `pure_pyflink_group_url_status_org_date_es.py`.

---

### 2. `pure_pyflink_group_url_status_org_date_es.py`

**Description:**
- Aggregates **URL and status code counts** from Kafka.
- Similar to `pur_pyflink_group_url_org_date_es.py` but focuses on **status code aggregation** as well.
- Optimized to reduce **bottlenecks** and improve **performance**.
- Designed for **continuous streaming** operation.
- Key challenges and bottlenecks discussed in comments:
  - Unbounded Flink state
  - High Elasticsearch upsert load
  - No retry handling
  - Large checkpoint size and slow recovery
  - Per-event REST overhead
  - Lack of backpressure awareness

---

### 3. `pure_pyflink_group_url_status_org_date_replace_es.py`

**Description:**
- Extends the logic from previous scripts.
- Replaces the **latest organization data** in Elasticsearch.
- Useful for cases where **aggregated data is already received** and needs to be replaced.

---

### 4. `pure_pyflink_with_totalurlcount_update_in_es.py`

**Description:**
- Reads streaming data from Kafka and performs **URL count aggregation**.
- Aggregates by `org_id` and `date`, then updates Elasticsearch with upsert logic.
- Uses a UDF to extract the date from incoming events.
- Designed for **real-time stream processing** with PyFlink.
- Continuously updates **total URL count** for each organization and date.
- 📌 For grouping based on `org_id`, `url`, and `date`, refer to `pur_pyflink_group_url_org_date_es.py`.

---

### 5. `buffer_deduplication_to_es.py`

## 📌 Key Features

- ✅ **Kafka Streaming Source**  
  - Consumes JSON logs from the `test-logs` Kafka topic.

- ✅ **Table API Aggregation**  
  - Aggregates log metrics using PyFlink Table API for:
    - `OrgUID + Date + URL` → `url_counts` index
    - `OrgUID + Date + Status` → `status_counts` index
    - Latest status/date per Org → `org_latest` index

- ✅ **Stateful Processing with TTL**  
  - Retains idle state for up to 1 hour to prevent memory bloat.

- ✅ **Changelog Stream Conversion**  
  - Converts table aggregates to changelog stream using `to_changelog_stream`.

- ✅ **Buffered Deduplicated Bulk Upserts**  
  - Uses dict-based buffers (not lists) to:
    - Hold only the latest update per document
    - Drop intermediate updates (e.g., count=1,2) before flushing count=3
    - Send to Elasticsearch in bulk every 20 updates

- ✅ **Custom Elasticsearch Sink (Python)**  
  - Uses native Elasticsearch `bulk()` API with `doc_as_upsert: true`
  - One sink per index: URL, status, and org replacement logic

- ✅ **Replace Latest Org Data**  
  - For each new log, replaces the latest `status` and `date` per org in a dedicated index

- ✅ **Fault Tolerance and Error Logging**  
  - Captures Elasticsearch failures without breaking Flink pipeline

---

## 🧱 Elasticsearch Indices

| Index          | Purpose                                      | Doc ID Format                      |
|----------------|----------------------------------------------|------------------------------------|
| `url_counts`   | Count per `org_id + date + url`              | `org1_2025-05-20_/v1_test`         |
| `status_counts`| Count per `org_id + date + status_code`      | `org1_2025-05-20_404`              |
| `org_latest`   | Replaces latest status/date for each org     | `org1`                             |

---

## ⚙️ Optimizations

- 🧠 **Bulk Upsert**: Minimizes network round-trips.
- 🧠 **Deduplication in Buffer**: Only final count per doc is retained before flush.
- 🧠 **State TTL**: Prevents unbounded Flink state growth.
- 🧠 **Document ID Strategy**: Ensures deterministic, idempotent upserts.

---


## 🛠 Technologies Used

- Apache Flink (PyFlink)
- Apache Kafka
- Elasticsearch
- Python (UDFs, Logging)
- Streaming Processing

---

## ✅ Use Cases

- Real-time analytics
- API usage monitoring
- Web traffic aggregation
- Log data processing
- Dashboard backends

---

## 📝 Notes

- All scripts are structured to support fault tolerance using Flink checkpointing.
- Make sure the required Kafka and Elasticsearch services are running before executing any script.
- UDFs are used for extracting relevant fields (e.g., date) from event payloads.
