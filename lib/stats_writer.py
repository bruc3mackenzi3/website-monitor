import json

from lib.kafka import Consumer
from lib.postgres import DbClient


class StatsWriter:
    '''
    Consumes messages from Kafka and writes stats to PostgreSQL DB.
    Interfaces with each through client wrapper libraries.
    '''

    def __init__(self, config: dict):
        self.consumer = Consumer(
                config['kafka']['server'],
                config['kafka']['topic']
        )
        self.db_client = DbClient(
                config['postgres']['host'],
                config['postgres']['db'],
                config['postgres']['username'],
                config['postgres']['table']
        )

    def consume_and_insert(self):
        while True:
            messages = self.consumer.consume()
            for raw_msg in messages:
                message = json.loads(raw_msg.value.decode('utf-8'))
                print(f'Consumer: got message {message}')

                # Extract timestamp from Kafka, converting miliseconds to seconds
                timestamp_s = int(raw_msg.timestamp/1000)

                self.db_client.insert(
                        message['site_url'],
                        message['http_status'],
                        message['response_time'],
                        timestamp_s
                )
