import json
import time
import random  # Add this line
from kafka import KafkaProducer

# Kafka configuration
bootstrap_servers = ['localhost:9092']  # Adjust to your Kafka setup
topic_name = 'ecommerce_events'

# Sample data (replace with more realistic data generation)
products = {
    1: {'name': 'Laptop', 'category': 'Electronics', 'description': 'Powerful laptop for students and professionals.'},
    2: {'name': 'T-shirt', 'category': 'Clothing', 'description': 'Comfortable and stylish t-shirt.'},
    3: {'name': 'Book', 'category': 'Books', 'description': 'Comprehensive textbook for college students.'},
    # ...
}


producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

while True:
    # Simulate user activity
    event_type = ['view', 'purchase'][random.randint(0, 1)] # view or purchase
    product_id = random.choice(list(products.keys()))
    user_id = random.randint(1, 100)  # Simulate different users

    event = {
        'event_type': event_type,
        'user_id': user_id,
        'product_id': product_id,
        'timestamp': int(time.time()),
    }

    try:
        producer.send(topic_name, value=event)
        print(f"Sent event: {event}")
    except KafkaTimeoutError as e:
        print(f"Error sending event: {e}")
    finally:
        time.sleep(random.uniform(1, 5)) # Simulate variable inter-event time
