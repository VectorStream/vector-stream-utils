import solara
import asyncio
from kafka_producer import KafkaProducer, products

# Global variable to store the Kafka producer instance
kafka_producer = KafkaProducer()

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

    solara.Button(label="Start Producer", on_click=start_producer, disabled=running.value)
    solara.Button(label="Stop Producer", on_click=stop_producer, disabled=not running.value)

    solara.Markdown(f"## Products")
    solara.DataFrame(products_list.value)

# Run the Solara app
solara.run_app(Page)
