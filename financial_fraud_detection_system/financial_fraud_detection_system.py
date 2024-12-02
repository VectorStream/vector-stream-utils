import asyncio
import logging
import random
from aiokafka import AIOKafkaProducer
from prometheus_client import start_http_server, Gauge
import os
import json

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")

account_numbers = ["123456789", "987654321", "112233445", "554433221", "667788990"]
account_holders = ["John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Davis"]
items = ["Groceries", "Electronics", "Clothing", "Furniture", "Travel"]
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", random.choice(account_numbers))

# Initialize Prometheus metrics
TRANSACTION_AMOUNT_GAUGE = Gauge('transaction_amount', 'Transaction Amount')
TRANSACTION_COUNT_GAUGE = Gauge('transaction_count', 'Transaction Count')
TRANSACTION_LATENCY_GAUGE = Gauge('transaction_latency', 'Transaction Latency')
RISK_SCORE_GAUGE = Gauge('risk_score', 'Risk Score')

async def produce_to_kafka(data):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    await producer.start()
    try:
        await producer.send(KAFKA_TOPIC, json.dumps(data).encode('utf-8'))
    finally:
        await producer.stop()

async def main():
    _logger = logging.getLogger("financial_fraud_detection")

    # Start Prometheus server
    start_http_server(8000)

    _logger.info("Starting financial transaction data producer!")

    transaction_count = 0

    while True:
        await asyncio.sleep(1)

        transaction_id = f"TXN{transaction_count:05d}"
        account_number = random.choice(account_numbers)
        account_holder = random.choice(account_holders)
        item = random.choice(items)
        amount = round(random.uniform(10.0, 10000.0), 2)
        transaction_latency = random.uniform(0.1, 2.0)
        risk_score = round(random.uniform(0.0, 1.0), 2)

        transaction_data = {
            "TransactionID": transaction_id,
            "AccountNumber": account_number,
            "AccountHolder": account_holder,
            "Item": item,
            "Amount": amount,
            "TransactionLatency": transaction_latency,
            "RiskScore": risk_score
        }

        await produce_to_kafka(transaction_data)

        # Update Prometheus metrics
        TRANSACTION_AMOUNT_GAUGE.set(amount)
        TRANSACTION_COUNT_GAUGE.inc()
        TRANSACTION_LATENCY_GAUGE.set(transaction_latency)
        RISK_SCORE_GAUGE.set(risk_score)

        _logger.info(f"Transaction: ID={transaction_id}, AccountNumber={account_number}, AccountHolder={account_holder}, Item={item}, Amount={amount}, Latency={transaction_latency}, RiskScore={risk_score}")

        transaction_count += 1

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    asyncio.run(main(), debug=True)
