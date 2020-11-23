#!/usr/bin/env python

import asyncio
import json
import threading
import sys

from lib.monitor import Monitor
from lib.stats_writer import StatsWriter

config = json.load(open('config.json'))


def run_monitor():
    monitor = Monitor(config)

    # NOTE: Ugly workaround for running asyncio on Windows
    # Solution adapted from here: https://github.com/encode/httpx/issues/914#issuecomment-622586610
    if (sys.version_info[0] == 3 and
    sys.version_info[1] >= 8 and
    sys.platform.startswith('win')):
        asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

    asyncio.run(monitor.monitor_and_produce(config['websites']))


def run_consumer():
    stats_writer = StatsWriter(config)
    stats_writer.consume_and_insert()


if __name__ == '__main__':
    try:
        monitor_thread = threading.Thread(target=run_monitor, daemon=True)
        consumer_thread = threading.Thread(target=run_consumer, daemon=True)

        monitor_thread.start()
        consumer_thread.start()

        # Join temporarily in loop to allow exiting via Ctrl+C from command line
        while monitor_thread.is_alive():
            monitor_thread.join(1)
        while consumer_thread.is_alive():
            consumer_thread.join(1)
    except KeyboardInterrupt:
        print('Ctrl+C pressed.  Exiting...')
        # TODO: close connections to Kafka and PostgreSQL
        sys.exit()
