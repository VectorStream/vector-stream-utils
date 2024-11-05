import asyncio
import aiokafka
import json
import random
import time
import logging
import socket

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('/tmp/kafka_producer.log')  # Changed log file path
    ]
)
logger = logging.getLogger(__name__)

# Kafka configuration
topic_name = 'ecommerceevents'

# Expanded product catalog
products = {
    1: {'name': 'Laptop', 'category': 'Electronics', 'price': 999.99},
    2: {'name': 'T-shirt', 'category': 'Clothing', 'price': 29.99},
    3: {'name': 'Book', 'category': 'Books', 'price': 14.99},
    4: {'name': 'Headphones', 'category': 'Electronics', 'price': 149.99},
    5: {'name': 'Jeans', 'category': 'Clothing', 'price': 59.99},
    6: {'name': 'Smartwatch', 'category': 'Electronics', 'price': 199.99},
    7: {'name': 'Running Shoes', 'category': 'Clothing', 'price': 89.99},
    8: {'name': 'Wireless Mouse', 'category': 'Electronics', 'price': 39.99},
    9: {'name': 'Hoodie', 'category': 'Clothing', 'price': 49.99},
    10: {'name': 'External SSD', 'category': 'Electronics', 'price': 129.99}
}

def get_kafka_bootstrap_server():
    """
    Attempt to resolve Kafka bootstrap server dynamically
    Supports multiple connection methods
    """
    # Method 1: Direct localhost (for local testing)
    local_servers = ['localhost:9092', '127.0.0.1:9092']
    
    # Method 2: Try resolving OpenShift service
    try:
        # Replace with your actual Kafka service name in OpenShift
        kafka_service_name = 'kafka-cluster-kafka-bootstrap'
        kafka_ip = socket.gethostbyname(kafka_service_name)
        local_servers.append(f"{kafka_ip}:9092")
    except Exception as e:
        logger.warning(f"Could not resolve Kafka service: {e}")
    
    return local_servers

class KafkaProducer:
    def __init__(self):
        self.producer = None
        self.running = False

    async def start(self):
        bootstrap_servers = get_kafka_bootstrap_server()
        logger.info(f"Attempting to connect to Kafka servers: {bootstrap_servers}")

        self.producer = aiokafka.AIOKafkaProducer(
            bootstrap_servers=bootstrap_servers,
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            client_id='ecommerce-producer',
            request_timeout_ms=5000,     # 5 seconds timeout
            max_block_ms=10000,          # 10 seconds max blocking time
            retry_backoff_ms=500         # 500ms between retries
        )

        await self.producer.start()
        logger.info("Kafka producer started successfully")
        self.running = True

        while self.running:
            try:
                event_type = random.choices(['view', 'purchase', 'cart_add'], 
                                             weights=[0.6, 0.3, 0.1])[0]
                product_id = random.choice(list(products.keys()))
                user_id = random.randint(1, 100)

                event = {
                    'event_type': event_type,
                    'user_id': user_id,
                    'product_id': product_id,
                    'product_name': products[product_id]['name'],
                    'product_category': products[product_id]['category'],
                    'product_price': products[product_id]['price'],
                    'timestamp': int(time.time()),
                    'session_id': random.randint(1000, 9999)
                }

                await self.producer.send_and_wait(topic_name, event)
                logger.info(f"Sent {event_type} event for Product: {event['product_name']}")

                await asyncio.sleep(random.uniform(0.5, 3))

            except Exception as send_error:
                logger.error(f"Error sending event: {send_error}")
                await asyncio.sleep(2)

    async def stop(self):
        if self.producer:
            await self.producer.stop()
            logger.info("Kafka producer stopped")
        self.running = False

    def is_running(self):
        return self.running
