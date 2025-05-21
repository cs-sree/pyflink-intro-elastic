import sys
import argparse
import json
from typing import Iterable

from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
from pyflink.common import Types, WatermarkStrategy, Time, Encoder
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction, WindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows, EventTimeSessionWindows, GlobalWindows


# Schema Definition
SCHEMA = {
    "EdgeEndTimestamp": str,
    "ClientRequestURI": str,
    "EdgeResponseStatus": int,
    "ResponseHeaders": {
        "x-org-uid": str
    }
}


# Custom Timestamp Assigner
class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        # Extract the timestamp in milliseconds
        import datetime
        timestamp = datetime.datetime.strptime(value['EdgeEndTimestamp'], "%Y-%m-%dT%H:%M:%SZ")
        return int(timestamp.timestamp() * 1000)


# Schema Validation Function
def validate_schema(record):
    try:
        # Check if all required keys exist and types match
        if not isinstance(record, dict):
            return False
        if 'EdgeEndTimestamp' not in record or not isinstance(record['EdgeEndTimestamp'], str):
            return False
        if 'ClientRequestURI' not in record or not isinstance(record['ClientRequestURI'], str):
            return False
        if 'EdgeResponseStatus' not in record or not isinstance(record['EdgeResponseStatus'], int):
            return False
        if 'ResponseHeaders' not in record or not isinstance(record['ResponseHeaders'], dict):
            return False
        if 'x-org-uid' not in record['ResponseHeaders'] or not isinstance(record['ResponseHeaders']['x-org-uid'], str):
            return False
        return True
    except Exception as e:
        print(f"Schema validation error: {e}")
        return False


# Window Function for URL Count per Org
class OrgUrlWindowFunction(ProcessWindowFunction[tuple, tuple, str, Time]):
    def process(self,
                key: str,
                context: ProcessWindowFunction.Context,
                elements: Iterable[tuple]) -> Iterable[tuple]:
        unique_urls = {}
        date = None
        for element in elements:
            if element['ClientRequestURI'] not in unique_urls:
                unique_urls[element['ClientRequestURI']] = 0
            unique_urls[element['ClientRequestURI']] += 1
            date = element['EdgeEndTimestamp']

        for url, count in unique_urls.items():
            yield (key, url, count, date)


# Window Function for Status Code Count per Org
class OrgStatusWindowFunction(ProcessWindowFunction[tuple, tuple, str, Time]):
    def process(self,
                key: str,
                context: ProcessWindowFunction.Context,
                elements: Iterable[tuple]) -> Iterable[tuple]:
        status_code_count = {}
        date = None
        for element in elements:
            status_code = element['EdgeResponseStatus']
            if status_code not in status_code_count:
                status_code_count[status_code] = 0
            status_code_count[status_code] += 1
            date = element['EdgeEndTimestamp']
        for status_code, count in status_code_count.items():
            yield (key, status_code, count, date)


def main(output_path=None):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Reading from JSON file
    file_path = '/Users/harshavardhan.reddy/Documents/ETL/pyflink-intro-elastic/apps/applied_practise/2021-03-27.json'

    with open(file_path, 'r') as f:
        json_data = [json.loads(line) for line in f if line.strip()]
    print(f"Loaded {len(json_data)} records from JSON file")

    # Filter data that matches the schema
    valid_data = [record for record in json_data if validate_schema(record)]
    print(f"Valid records found: {len(valid_data)}")

    # Create a DataStream from the valid JSON data
    data_stream = env.from_collection(
        [json.dumps(record) for record in valid_data],
        type_info=Types.STRING()
    )

    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
        .with_timestamp_assigner(MyTimestampAssigner())

    # Assign Timestamps and Watermarks
    ds = data_stream.map(lambda x: json.loads(x)) \
        .assign_timestamps_and_watermarks(watermark_strategy) \
        .key_by(lambda x: x['ResponseHeaders']['x-org-uid'], key_type=Types.STRING())

    print('--- URL Count Per Org ---')
    # Apply Tumbling Window for URLs (10 seconds)
    url_window = ds.window(TumblingEventTimeWindows.of(Time.seconds(10))) \
        .process(OrgUrlWindowFunction(), Types.TUPLE([Types.STRING(), Types.STRING(), Types.INT(), Types.STRING()]))

    url_window.print()

    print('--- Status Code Count Per Org ---')
    # Apply Tumbling Window for Status Codes (10 seconds)
    status_window = ds.window(TumblingEventTimeWindows.of(Time.seconds(10))) \
        .process(OrgStatusWindowFunction(), Types.TUPLE([Types.STRING(), Types.INT(), Types.INT(), Types.STRING()]))

    status_window.print()

    env.execute()


if __name__ == '__main__':
    main()
""