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
    3: {'name': 'Book', 'category': 'Books', 'description': 'Engaging novel for fiction lovers.'},
    4: {'name': 'Headphones', 'category': 'Electronics', 'description': 'Noise-canceling headphones for immersive audio.'},
    5: {'name': 'Jeans', 'category': 'Clothing', 'description': 'Durable and fashionable jeans.'},
    6: {'name': 'Coffee Maker', 'category': 'Home Appliances', 'description': 'Automatic coffee maker for a perfect morning brew.'},
    7: {'name': 'Smartwatch', 'category': 'Electronics', 'description': 'Fitness tracker smartwatch with heart rate monitoring.'},
    8: {'name': 'Sweater', 'category': 'Clothing', 'description': 'Cozy sweater for cold weather.'},
    9: {'name': 'Notebook', 'category': 'Stationery', 'description': 'High-quality notebook for taking notes.'},
    10: {'name': 'Tablet', 'category': 'Electronics', 'description': 'Portable tablet for entertainment and productivity.'},
    11: {'name': 'Sneakers', 'category': 'Clothing', 'description': 'Comfortable running sneakers.'},
    12: {'name': 'Blouse', 'category': 'Clothing', 'description': 'Elegant blouse for formal occasions.'},
    13: {'name': 'Cookbook', 'category': 'Books', 'description': 'Collection of delicious recipes.'},
    14: {'name': 'Wireless Mouse', 'category': 'Electronics', 'description': 'Ergonomic wireless mouse for comfortable computing.'},
    15: {'name': 'Dress', 'category': 'Clothing', 'description': 'Stylish dress for any occasion.'},
    16: {'name': 'Textbook', 'category': 'Books', 'description': 'Comprehensive textbook for college students.'},
    17: {'name': 'Smart TV', 'category': 'Electronics', 'description': 'Large screen smart TV with streaming capabilities.'},
    18: {'name': 'Pants', 'category': 'Clothing', 'description': 'Versatile pants ideal for work or casual wear.'},
    19: {'name': 'Self-Help Book', 'category': 'Books', 'description': 'Motivational self-help book.'},
    20: {'name': 'Keyboard', 'category': 'Electronics', 'description': 'Mechanical keyboard for improved typing experience.'},
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

    producer.send(topic_name, value=event)
    print(f"Sent event: {event}")
    time.sleep(random.uniform(1, 5)) # Simulate variable inter-event time
