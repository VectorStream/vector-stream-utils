import solara
import asyncio
from kafka_producer import KafkaProducer, products

# Global variable to store the Kafka producer instance
kafka_producer = KafkaProducer()

# Reactive counter
clicks = solara.reactive(0)

@solara.component
def Page():
    # State variables
    running = solara.use_state(False)
    products_list = solara.use_state(list(products.values()))

    async def start_producer():
        running.set(True)
        await kafka_producer.start()

    async def stop_producer():
        running.set(False)
        await kafka_producer.stop()

    def increment():
        clicks.value += 1
        print("clicks", clicks.value)

    label = "Not clicked yet" if clicks.value == 0 else f"Clicked: {clicks.value}"
    solara.Button(label=label, on_click=increment, color="green" if clicks.value < 5 else "red")

    solara.Button(label="Start Producer", on_click=start_producer, disabled=running.value)
    solara.Button(label="Stop Producer", on_click=stop_producer, disabled=not running.value)

    solara.Markdown(f"## Products")
    solara.DataFrame(products_list.value)

# Run the Solara app
solara.start_server(Page)
