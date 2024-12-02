import asyncio
import logging
import random
from aiokafka import AIOKafkaProducer
from prometheus_client import start_http_server, Gauge
import os
import json

KAFKA_BROKER = os.environ.get("KAFKA_BROKER", "localhost:9092")

account_numbers = [
    "123456789", "987654321", "112233445", "554433221", "667788990",
    "342109876", "765432198", "654321098", "219876543", "432198765",
    "111111111", "222222222", "333333333", "444444444", "555555555",
    "123123123", "456456456", "789789789", "012345678", "987654320"
]

account_holders = [
    "John Doe", "Jane Smith", "Alice Johnson", "Bob Brown", "Charlie Davis",
    "Emily Chen", "Liam Kim", "Ava Morales", "Ethan Hall", "Sophia Patel",
    "Noah Lee", "Mia Garcia", "Logan Brooks", "Isabella Martin", "Alexander White",
    "Charlotte Taylor", "Benjamin Lewis", "Harper Walker", "Gabriel Russell", "Abigail Jenkins"
]

items  = [
    "Rent/Mortgage", "Utilities", "Groceries", "Entertainment", "Dining Out",
    "Gas", "Car Payment", "Insurance", "Subscription Services", "Home Improvement",
    "Electronics", "Clothing", "Furniture", "Home Goods", "Travel",
    "Charitable Donations", "Miscellaneous", "Public Transport", "Takeaway"
]
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC", random.choice(account_numbers))

# Initialize Prometheus metrics
TRANSACTION_AMOUNT_GAUGE = Gauge('transaction_amount', 'Transaction Amount')
TRANSACTION_COUNT_GAUGE = Gauge('transaction_count', 'Transaction Count')
TRANSACTION_LATENCY_GAUGE = Gauge('transaction_latency', 'Transaction Latency')
RISK_SCORE_GAUGE = Gauge('risk_score', 'Risk Score')

TRANSACTION_INTERVAL = float(os.environ.get("TRANSACTION_INTERVAL", 1.0))
FRAUD_PERCENTAGE = float(os.environ.get("FRAUD_PERCENTAGE", 2.0))

# Map account numbers to account holders
account_mapping = {
    "123456789": "John Doe", "987654321": "Jane Smith", "112233445": "Alice Johnson", 
    "554433221": "Bob Brown", "667788990": "Charlie Davis", "342109876": "Emily Chen", 
    "765432198": "Liam Kim", "654321098": "Ava Morales", "219876543": "Ethan Hall", 
    "432198765": "Sophia Patel", "111111111": "Noah Lee", "222222222": "Mia Garcia", 
    "333333333": "Logan Brooks", "444444444": "Isabella Martin", "555555555": "Alexander White", 
    "123123123": "Charlotte Taylor", "456456456": "Benjamin Lewis", "789789789": "Harper Walker", 
    "012345678": "Gabriel Russell", "987654320": "Abigail Jenkins"
}

async def produce_to_kafka(data, topic):
    producer = AIOKafkaProducer(bootstrap_servers=KAFKA_BROKER)
    await producer.start()
    try:
        key = data["TransactionID"].encode('utf-8')
        headers = [
            ("account_number", data["AccountNumber"].encode('utf-8')),
            ("account_holder", data["AccountHolder"].encode('utf-8')),
            ("item", data["Item"].encode('utf-8'))
        ]
        await producer.send(topic, json.dumps(data).encode('utf-8'), key=key, headers=headers)
    finally:
        await producer.stop()

async def main():
    _logger = logging.getLogger("financial_fraud_detection")

    # Start Prometheus server
    start_http_server(8000)

    _logger.info("Starting financial transaction data producer!")

    transaction_count = 0

    while True:
        await asyncio.sleep(TRANSACTION_INTERVAL)

        transaction_id = f"TXN{transaction_count:05d}"
        account_number = random.choice(account_numbers)
        
        # Determine if this transaction should be fraudulent
        if random.uniform(0, 100) < FRAUD_PERCENTAGE:
            account_holder = random.choice(account_holders)
        else:
            account_holder = account_mapping[account_number]
        
        item = random.choice(items)
        amount = round(random.uniform(10.0, 10000.0), 2)
        transaction_latency = random.uniform(0.1, 2.0)
        risk_score = round(random.uniform(0.0, 1.0), 2)

        # Check for potential fraud
        expected_account_holder = account_mapping.get(account_number)
        if account_holder != expected_account_holder:
            _logger.warning(f"Potential fraud detected: AccountNumber={account_number} does not match AccountHolder={account_holder}")

        transaction_data = {
            "TransactionID": transaction_id,
            "AccountNumber": account_number,
            "AccountHolder": account_holder,
            "Item": item,
            "Amount": amount,
            "TransactionLatency": transaction_latency,
            "RiskScore": risk_score
        }

        await produce_to_kafka(transaction_data, account_number)

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
