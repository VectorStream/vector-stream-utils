import unittest
from kafka_producer import get_kafka_bootstrap_server, KafkaProducer

class TestKafkaProducer(unittest.TestCase):

    def test_get_kafka_bootstrap_server(self):
        server = get_kafka_bootstrap_server()
        self.assertIsInstance(server, str)

    def test_kafka_producer_initialization(self):
        producer = KafkaProducer()
        self.assertIsNone(producer.producer)

if __name__ == '__main__':
    unittest.main()
