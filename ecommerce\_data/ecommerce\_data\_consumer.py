import asyncio
import aiokafka
import json
import random
import time
import logging
import socket

// Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler('kafka_producer.txt')
    ]
)
logger = logging.getLogger(__name__)

// Kafka configuration
topic_name = 'ecommerceevents'

// Expanded product catalog
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

// ... (rest of the script remains unchanged)
