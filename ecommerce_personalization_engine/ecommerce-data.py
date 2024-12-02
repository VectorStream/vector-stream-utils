import asyncio
import logging
import random
from asyncua import Server, ua
from aiokafka import AIOKafkaProducer
from prometheus_client import start_http_server, Counter, Gauge
import os
import json

# Kafka Setup
KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")
KAFKA_TOPIC_BROWSING = os.environ.get("KAFKA_TOPIC_BROWSING", "comic_book_browsing")
KAFKA_TOPIC_PURCHASES = os.environ.get("KAFKA_TOPIC_PURCHASES", "comic_book_purchases")

# Comic Book Store Inventory (Simplified)
inventory = {
    "BATMAN#1": {"title": "Batman", "issue": 1, "price": 4.99},
    "SPIDERMAN#10": {"title": "Spider-Man", "issue": 10, "price": 3.99},
    "GRAPHIC_NOVEL_AVENGERS": {"title": "Avengers Graphic Novel", "price": 19.99},
    "MERCH_HARLEY_QUINN_PIN": {"title": "Harley Quinn Enamel Pin", "price": 9.99},
    
    # Additional Comics
    "SUPERMAN#5": {"title": "Superman", "issue": 5, "price": 4.49},
    "WONDERWOMAN#2": {"title": "Wonder Woman", "issue": 2, "price": 4.99},
    "FLASH#8": {"title": "The Flash", "issue": 8, "price": 3.99},
    "GREENLANTERN#12": {"title": "Green Lantern", "issue": 12, "price": 4.49},
    "AQUAMAN#6": {"title": "Aquaman", "issue": 6, "price": 4.99},
    "CYBORG#4": {"title": "Cyborg", "issue": 4, "price": 3.99},
    "SHAZAM#10": {"title": "Shazam", "issue": 10, "price": 4.49},
    "HARLEYQUINN#1": {"title": "Harley Quinn", "issue": 1, "price": 4.99},
    "POISONIVY#2": {"title": "Poison Ivy", "issue": 2, "price": 4.49},
    "CATWOMAN#5": {"title": "Catwoman", "issue": 5, "price": 3.99},
    
    # More Graphic Novels
    "GRAPHIC_NOVEL_JUSTICELEAGUE": {"title": "Justice League Graphic Novel", "price": 24.99},
    "GRAPHIC_NOVEL_BATMAN/#{THEKILLINGJOKE}": {"title": "Batman: The Killing Joke Graphic Novel", "price": 14.99},
    "GRAPHIC_NOVEL_SUPERMAN/#{REDSON}": {"title": "Superman: Red Son Graphic Novel", "price": 19.99},
    "GRAPHIC_NOVEL_WONDERWOMAN}/#{BLOOD}": {"title": "Wonder Woman: Blood Graphic Novel", "price": 17.99},
    "GRAPHIC_NOVEL/#{THEWATCHMEN}": {"title": "Watchmen Graphic Novel", "price": 29.99},
    
    # Additional Merchandise
    "MERCH_SUPERMAN_TSHIRT": {"title": "Superman Logo T-Shirt", "price": 19.99},
    "MERCH_BATMAN_POSTER": {"title": "Batman Poster (24\" x 36\")", "price": 14.99},
    "MERCH_WONDERWOMAN_MUG": {"title": "Wonder Woman Logo Mug", "price": 12.99},
    "MERCH_JUSTICELEAGUE님의FIGURINE": {"title": "Justice League 5-Pack Figurine Set", "price": 49.99},
    "MERCH_HARLEYQUINN_#{SQUEEZEETOY}": {"title": "Harley Quinn Squeeze-E Toy", "price": 7.99},
    "MERCH_COMIC_BOOK_BOX": {"title": "Short Box for Comic Book Storage", "price": 10.99},
    "MERCH_GRAPHIC_NOVEL_BOOKSHELF": {"title": "Bookshelf for Graphic Novel Display", "price": 39.99},
    
    # Even More Comics
    "TEEN ITEM#3": {"title": "Teen Titans", "issue": 3, "price": 4.49},
    "ROBIN#9": {"title": "Robin", "issue": 9, "price": 4.99},
    "NIGHTWING#11": {"title": "Nightwing", "issue": 11, "price": 4.49},
    "REDHOOD#7": {"title": "Red Hood", "issue": 7, "price": 3.99},
    "REDROBIN#4": {"title": "Red Robin", "issue": 4, "price": 4.99},
    
    # Final Additions
    "MERA#1": {"title": "Mera", "issue": 1, "price": 4.49},
    "BATGIRL#6": {"title": "Batgirl", "issue": 6, "price": 4.99},
    "ZATANNATHEDAY#3": {"title": "Zatanna the Day", "issue": 3, "price": 3.99},
    "SIRENS#10": {"title": "Sirens", "issue": 10, "price": 4.49},
    "BIRDSOFPREY#5": {"title": "Birds of Prey", "issue": 5, "price": 4.99},
}


# Prometheus Metrics
BROWSING_EVENTS = Counter('browsing_events', 'Number of browsing events')
COMIC_PURCHASES = Counter('comic_purchases', 'Number of comic book purchases')
GRAPHIC_NOVEL_PURCHASES = Counter('graphic_novel_purchases', 'Number of graphic novel purchases')
MERCH_PURCHASES = Counter('merchandise_purchases', 'Number of merchandise purchases')
REVENUE_GAUGE = Gauge('revenue', 'Total revenue from purchases')

async def produce_to_kafka(topic, data):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    await producer.start()
    try:
        key = data["item_id"].encode('utf-8')
        headers = [
            ("content-type", b"application/json"),
            ("event-type", data["action"].encode('utf-8') if "action" in data else b"purchase")
        ]
        await producer.send(topic, json.dumps(data).encode('utf-8'), key=key, headers=headers)
    finally:
        await producer.stop()

async def simulate_customer_interaction():
    # Simulate Browsing
    browse_item_id = random.choice(list(inventory.keys()))
    browsing_data = {
        "timestamp": str(asyncio.get_event_loop().time()),
        "item_id": browse_item_id,
        "item_title": inventory[browse_item_id]["title"],
        "action": "browse"
    }
    await produce_to_kafka(KAFKA_TOPIC_BROWSING, browsing_data)
    BROWSING_EVENTS.inc()
    logging.info(f"Browsing Event: {browsing_data}")

    # Simulate Purchase with 20% chance
    if random.random() < 0.2:
        purchase_item_id = browse_item_id  # Assume the browsed item is purchased for simplicity
        if "issue" in inventory[purchase_item_id]:
            purchase_type = "comic"
            COMIC_PURCHASES.inc()
        elif "Graphic Novel" in inventory[purchase_item_id]["title"]:
            purchase_type = "graphic_novel"
            GRAPHIC_NOVEL_PURCHASES.inc()
        else:
            purchase_type = "merchandise"
            MERCH_PURCHASES.inc()
        
        purchase_data = {
            "timestamp": str(asyncio.get_event_loop().time()),
            "item_id": purchase_item_id,
            "item_title": inventory[purchase_item_id]["title"],
            "price": inventory[purchase_item_id]["price"],
            "purchase_type": purchase_type
        }
        await produce_to_kafka(KAFKA_TOPIC_PURCHASES, purchase_data)
        REVENUE_GAUGE.inc(inventory[purchase_item_id]["price"])
        logging.info(f"Purchase Event: {purchase_data}")

# Interaction interval setup
INTERACTION_INTERVAL = float(os.environ.get("INTERACTION_INTERVAL", 1.0))

async def main():
    _logger = logging.getLogger("asyncua")
    # Start Prometheus server
    start_http_server(8000)

    # Simple loop to continuously simulate customer interactions
    while True:
        await asyncio.sleep(random.uniform(0.1, INTERACTION_INTERVAL))  # Simulate interactions at varying rates
        await simulate_customer_interaction()

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main(), debug=True)
