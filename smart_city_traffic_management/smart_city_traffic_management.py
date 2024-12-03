import asyncio
import json
import random
import os
import logging
from aiokafka import AIOKafkaProducer
from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Kafka configuration
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "traffic_data")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

INTERVAL = int(os.getenv("INTERVAL", 1))
if INTERVAL <= 0:
    raise ValueError("INTERVAL must be a positive integer.")

# Function to ensure the Kafka topic exists
def ensure_kafka_topic():
    admin_client = KafkaAdminClient(bootstrap_servers=KAFKA_BROKER)
    topic = NewTopic(name=KAFKA_TOPIC, num_partitions=1, replication_factor=1)
    try:
        admin_client.create_topics([topic])
        logger.info(f"Topic '{KAFKA_TOPIC}' created.")
    except TopicAlreadyExistsError:
        logger.info(f"Topic '{KAFKA_TOPIC}' already exists.")
    finally:
        admin_client.close()

# Function to send data to Kafka
async def produce_to_kafka(producer, data):
    try:
        key = str(data["Time of Day"]).encode()
        headers = [("content-type", b"application/json")]
        await producer.send_and_wait(KAFKA_TOPIC, json.dumps(data).encode(), key=key, headers=headers)
        logger.info(f"Data sent to Kafka: {data}")
    except Exception as e:
        logger.error(f"Error sending data to Kafka: {e}")

# Main function to simulate data and send it to Kafka
async def main():
    # Ensure the Kafka topic exists
    ensure_kafka_topic()

    # Create a Kafka producer
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    await producer.start()
    logger.info("Kafka producer started.")

    try:
        current_state_index = 0  # For traffic signal state machine

        while True:
            await asyncio.sleep(INTERVAL)

            # Constants and Lists
            location_types = ["Downtown", "Suburbs", "Airport", "Train Station"]
            traffic_signal_states = ["red", "yellow", "green"]
            transportation_modes = ["Cars", "Taxis", "Ride-hailing", "Buses", "Trains", "Planes"]
            time_of_day = random.choice(["Peak", "Off-Peak", "Night"])  # Simplified time of day
            weather_conditions = ["Sunny", "Cloudy", "Raining", "Snowing"]

            # Simulated Data Generation
            traffic_volume = random.randint(0, 100)
            traffic_speed = max(0, random.uniform(60 - (traffic_volume * 0.6), 60))
            traffic_occupancy = min(1, traffic_volume / 100 + random.uniform(-0.1, 0.1))

            public_transit_location = random.choice(location_types)
            public_transit_passengers = (
                random.randint(50, 100)
                if public_transit_location == "Downtown" and time_of_day == "Peak"
                else random.randint(0, 50)
            )

            pedestrian_count = random.randint(0, 100)
            bicycle_count = random.randint(0, 50)

            car_count = random.randint(0, traffic_volume)
            taxi_count = random.randint(0, int(traffic_volume / 5))
            ride_hailing_count = random.randint(0, int(traffic_volume / 3))
            bus_count = random.randint(0, int(public_transit_passengers / 20))

            train_location = random.choice(["Train Station", "Suburbs", "Downtown"])
            train_passengers = random.randint(0, 500) if train_location == "Train Station" else random.randint(0, 100)
            train_arrivals = random.randint(0, 10) if time_of_day == "Peak" else random.randint(0, 2)

            plane_location = "Airport"
            plane_arrivals = random.randint(5, 20) if time_of_day == "Peak" else random.randint(0, 5)
            plane_passengers = random.randint(50, 200) * plane_arrivals

            traffic_signal_state = traffic_signal_states[current_state_index % len(traffic_signal_states)]
            current_state_index += 1

            weather = random.choice(weather_conditions)
            if weather in ["Raining", "Snowing"]:
                traffic_speed *= 0.7
                plane_arrivals = max(0, int(plane_arrivals * 0.8))
                train_arrivals = max(0, int(train_arrivals * 0.9))

            kafka_data = {
                "Road Transportation": {
                    "TrafficVolume": traffic_volume,
                    "TrafficSpeed": traffic_speed,
                    "TrafficOccupancy": traffic_occupancy,
                    "PublicTransitLocation": public_transit_location,
                    "PublicTransitPassengers": public_transit_passengers,
                    "PedestrianCount": pedestrian_count,
                    "BicycleCount": bicycle_count,
                    "Automobiles": {
                        "Cars": car_count,
                        "Taxis": taxi_count,
                        "Ride-hailing": ride_hailing_count,
                        "Buses": bus_count
                    }
                },
                "Rail Transportation": {
                    "TrainLocation": train_location,
                    "TrainPassengers": train_passengers,
                    "TrainArrivals": train_arrivals
                },
                "Air Transportation": {
                    "PlaneLocation": plane_location,
                    "PlaneArrivals": plane_arrivals,
                    "PlanePassengers": plane_passengers
                },
                "Traffic Signal State": traffic_signal_state,
                "Time of Day": time_of_day,
                "Weather Conditions": weather
            }

            await produce_to_kafka(producer, kafka_data)

    except KeyboardInterrupt:
        logger.info("Stopping producer.")
    finally:
        await producer.stop()
        logger.info("Kafka producer stopped.")

if __name__ == "__main__":
    asyncio.run(main())
