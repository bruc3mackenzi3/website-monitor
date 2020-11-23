import json

from lib.kafka import Producer, Consumer

config = json.load(open('config.json'))


class TestKafkaClient:
    test_topic = 'test_monitor_events'

    # TODO: Test connection to Kafka broker

    def test_message_e2e(self):
        message = 'this is a test message'.encode('utf-8')

        # This also tests Kafka broker connection
        client = Producer(config['kafka']['server'], self.test_topic)
        client.produce(message)
        client.producer.flush()

        consumer = Consumer(config['kafka']['server'], self.test_topic)
        messages = consumer.consume()
        # NOTE: Only check most recent value consumed
        assert messages[-1].value == message
