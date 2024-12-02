import asyncio
import json
import random
import os
from aiokafka import AIOKafkaProducer

# Kafka configuration
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC", "traffic_data")
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "localhost:9092")

INTERVAL = int(os.getenv("INTERVAL", 1))

async def produce_to_kafka(data):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    await producer.start()
    await producer.send_and_wait(KAFKA_TOPIC, json.dumps(data).encode())
    await producer.stop()

async def main():
    while True:
        await asyncio.sleep(INTERVAL)

        # Constants and Lists
        location_types = ["Downtown", "Suburbs", "Airport", "Train Station"]
        traffic_signal_states = ["red", "yellow", "green"]
        transportation_modes = ["Cars", "Taxis", "Ride-hailing", "Buses", "Trains", "Planes"]
        time_of_day = random.choice(["Peak", "Off-Peak", "Night"])  # Simplified time of day
        current_state_index = 0  # For traffic signal state machine
        weather_conditions = ["Sunny", "Cloudy", "Raining", "Snowing"]

        # Simulated Data Generation
        ## Road Transportation
        traffic_volume = random.randint(0, 100)
        traffic_speed = random.uniform(60 - (traffic_volume * 0.6), 60) if traffic_volume < 30 else random.uniform(0, 60 - (traffic_volume * 0.6))
        traffic_occupancy = min(1, traffic_volume / 100 + random.uniform(-0.1, 0.1))

        public_transit_location = random.choice(location_types)
        if public_transit_location == "Downtown" and time_of_day == "Peak":
            public_transit_passengers = random.randint(50, 100)
        else:
            public_transit_passengers = random.randint(0, 50)

        pedestrian_count = random.randint(0, 100)
        bicycle_count = random.randint(0, 50)

        ## Automobiles
        car_count = random.randint(0, traffic_volume)
        taxi_count = random.randint(0, int(traffic_volume / 5))  # Simplified assumption
        ride_hailing_count = random.randint(0, int(traffic_volume / 3))  # Simplified assumption
        bus_count = random.randint(0, int(public_transit_passengers / 20))  # Simplified assumption

        ## Trains
        train_location = random.choice(["Train Station", "Suburbs", "Downtown"])
        train_passengers = random.randint(0, 500) if train_location == "Train Station" else random.randint(0, 100)
        train_arrivals = random.randint(0, 10) if time_of_day == "Peak" else random.randint(0, 2)

        ## Planes
        plane_location = "Airport"
        plane_arrivals = random.randint(5, 20) if time_of_day == "Peak" else random.randint(0, 5)
        plane_passengers = random.randint(50, 200) * plane_arrivals

        ## Traffic Signal
        traffic_signal_state = traffic_signal_states[current_state_index % len(traffic_signal_states)]
        # Increment for next iteration
        current_state_index += 1

        ## Weather (Influences all modes)
        weather = random.choice(weather_conditions)
        if weather == "Raining" or weather == "Snowing":
            traffic_speed = traffic_speed * 0.7  # Reduce speed due to weather
            plane_arrivals = plane_arrivals * 0.8  # Delayed arrivals
            train_arrivals = train_arrivals * 0.9  # Minor delays

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

        # print(kafka_data)  # Uncomment to see the generated data

        await produce_to_kafka(kafka_data)

if __name__ == "__main__":
    asyncio.run(main())
