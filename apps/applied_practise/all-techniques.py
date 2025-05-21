import sys
import argparse
import json
from typing import Iterable

from pyflink.datastream.connectors.file_system import FileSink, OutputFileConfig, RollingPolicy
from pyflink.common import Types, WatermarkStrategy, Time, Encoder
from pyflink.common.watermark_strategy import TimestampAssigner
from pyflink.datastream import StreamExecutionEnvironment, ProcessWindowFunction, WindowFunction
from pyflink.datastream.window import TumblingEventTimeWindows, SlidingEventTimeWindows, EventTimeSessionWindows, GlobalWindows

# Custom Timestamp Assigner
class MyTimestampAssigner(TimestampAssigner):
    def extract_timestamp(self, value, record_timestamp) -> int:
        return int(value['timestamp'])

# Custom Window Function for Counting URLs
class URLCountWindowFunction(ProcessWindowFunction[tuple, tuple, str, Time]):
    def process(self,
                key: str,
                context: ProcessWindowFunction.Context,
                elements: Iterable[tuple]) -> Iterable[tuple]:
        count = len([e for e in elements])
        # Enhanced: Including status code and URL count
        status_codes = {}
        for element in elements:
            status = element['status_code'] if 'status_code' in element else 'Unknown'
            if status not in status_codes:
                status_codes[status] = 0
            status_codes[status] += 1

        for status, scount in status_codes.items():
            yield (key, status, scount, count)

# Custom Sum Function for Global Window
class GlobalSumFunction(WindowFunction[tuple, tuple, str, Time]):
    def apply(self, key: str, window, inputs: Iterable[tuple]):
        total = 0
        for record in inputs:
            total += record[3]
        yield ('Total Count for URL', key, total)

def main(output_path=None):
    env = StreamExecutionEnvironment.get_execution_environment()
    env.set_parallelism(1)

    # Sample Data Stream
    data_stream = env.from_collection([
        {'timestamp': 1000, 'url': '/v3/organizations/blt11f5b5413ea9c95a', 'status_code': 200},
        {'timestamp': 2000, 'url': '/v3/stacks/blt41a9aea6e5df3c94/recent_search', 'status_code': 200},
        {'timestamp': 3000, 'url': '/v3/organizations/blt11f5b5413ea9c95a', 'status_code': 404},
        {'timestamp': 4000, 'url': '/v3/entries', 'status_code': 200},
        {'timestamp': 8000, 'url': '/v3/organizations/blt11f5b5413ea9c95a', 'status_code': 500},
        {'timestamp': 9000, 'url': '/v3/stacks/blt41a9aea6e5df3c94/recent_search', 'status_code': 502},
        {'timestamp': 15000, 'url': '/v3/entries', 'status_code': 200}
    ], type_info=Types.MAP(Types.STRING(), Types.STRING()))

    watermark_strategy = WatermarkStrategy.for_monotonous_timestamps() \
        .with_timestamp_assigner(MyTimestampAssigner())

    # Assign Timestamps and Watermarks
    ds = data_stream.assign_timestamps_and_watermarks(watermark_strategy) \
        .key_by(lambda x: x['url'], key_type=Types.STRING())

    # Apply Tumbling Window (5 seconds)
    tumbling_window = ds.window(TumblingEventTimeWindows.of(Time.seconds(5))) \
        .process(URLCountWindowFunction(), Types.TUPLE([Types.STRING(), Types.STRING(), Types.INT(), Types.INT()]))

    # Apply Sliding Window (5 seconds window, 2 seconds slide)
    sliding_window = ds.window(SlidingEventTimeWindows.of(Time.seconds(5), Time.seconds(2))) \
        .process(URLCountWindowFunction(), Types.TUPLE([Types.STRING(), Types.STRING(), Types.INT(), Types.INT()]))

    # Apply Session Window (5 seconds gap)
    session_window = ds.window(EventTimeSessionWindows.with_gap(Time.seconds(5))) \
        .process(URLCountWindowFunction(), Types.TUPLE([Types.STRING(), Types.STRING(), Types.INT(), Types.INT()]))

    # Apply Global Window for Final Sum
    total_sum = tumbling_window \
        .window_all(GlobalWindows.create()) \
        .process(GlobalSumFunction(), Types.TUPLE([Types.STRING(), Types.STRING(), Types.INT()]))

    # Define the sink
    if output_path is not None:
        total_sum.sink_to(
            sink=FileSink.for_row_format(
                base_path=output_path,
                encoder=Encoder.simple_string_encoder())
            .with_output_file_config(
                OutputFileConfig.builder()
                .with_part_prefix('prefix')
                .with_part_suffix('.ext')
                .build())
            .with_rolling_policy(RollingPolicy.default_rolling_policy())
            .build()
        )
    else:
        print('--- Tumbling Window Results ---')
        tumbling_window.print()
        # sliding_window.print()
        # session_window.print()
        # total_sum.print()

    env.execute()

if __name__ == '__main__':
    main()
