import unittest
from unittest.mock import patch, ANY
import asyncio
from kafka_producer import get_kafka_bootstrap_server, KafkaProducer

class TestKafkaProducer(unittest.TestCase):

    def test_get_kafka_bootstrap_server(self):
        server = get_kafka_bootstrap_server()
        self.assertIsInstance(server, str)

    @patch('aiokafka.AIOKafkaProducer')
    def test_kafka_producer_start(self, mock_producer):
        producer = KafkaProducer()
        mock_producer_instance = mock_producer.return_value
        mock_producer_instance.start.return_value = None

        loop = asyncio.get_event_loop()
        loop.run_until_complete(producer.start())

        mock_producer.assert_called_once_with(
            bootstrap_servers=get_kafka_bootstrap_server(),
            value_serializer=ANY,
            client_id='ecommerce-producer',
            request_timeout_ms=5000,
            max_block_ms=10000,
            retry_backoff_ms=500
        )
        mock_producer_instance.start.assert_called_once()
        self.assertTrue(producer.running)

if __name__ == '__main__':
    unittest.main()
