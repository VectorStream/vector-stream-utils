# Kafka-LLM Integration Demos

This repository showcases five fictional demo use cases that highlight the business and technical value of integrating Apache Kafka with Large Language Models (LLMs). Each use case includes Python scripts demonstrating real-time data streaming and processing.

## Table of Contents

1. [E-commerce Personalization Engine](#1-e-commerce-personalization-engine)
2. [Financial Fraud Detection System](#2-financial-fraud-detection-system)
3. [Smart City Traffic Management](#3-smart-city-traffic-management)
4. [Healthcare Patient Monitoring System](#4-healthcare-patient-monitoring-system)
5. [Manufacturing Quality Control](#5-manufacturing-quality-control)

## Prerequisites

- Python 3.x
- Apache Kafka
- Required Python libraries (specified in each use case)

## Setup Instructions

1. **Install Apache Kafka**: Follow the official [Kafka Quickstart guide](https://kafka.apache.org/quickstart) to set up Kafka on your system.
2. **Install Python Dependencies**: Navigate to each use case directory and install the necessary Python libraries using `pip install -r requirements.txt`.

## Use Cases

### 1. E-commerce Personalization Engine

**Description**: Provides real-time personalized product recommendations to customers by processing browsing data and purchase history.

**Scripts**:
- `ecommerce_data_producer.py`: Simulates customer browsing and purchase data and sends it to Kafka topics.
- `ecommerce_data_consumer.py`: Consumes data from Kafka, processes it, and interacts with the LLM to generate recommendations.

**Dependencies**:
- `kafka-python`
- `transformers` (for LLM integration)

**Usage**:
```bash
python ecommerce_data_producer.py
python ecommerce_data_consumer.py
```

### 2. Financial Fraud Detection System

**Description**: Detects and prevents fraudulent transactions in real-time by analyzing transaction patterns.

**Scripts**:
- `financial_data_producer.py`: Generates simulated transaction data and publishes it to Kafka.
- `fraud_detection_consumer.py`: Consumes transaction data, processes it, and uses the LLM to identify potential fraud.

**Dependencies**:
- `kafka-python`
- `scikit-learn` (for data processing)
- `transformers`

**Usage**:
```bash
python financial_data_producer.py
python fraud_detection_consumer.py
```

### 3. Smart City Traffic Management

**Description**: Optimizes traffic flow by processing data from traffic sensors and public transit systems.

**Scripts**:
- `traffic_data_producer.py`: Simulates traffic sensor and public transit data, sending it to Kafka.
- `traffic_management_consumer.py`: Consumes traffic data, processes it, and uses the LLM to suggest traffic optimizations.

**Dependencies**:
- `kafka-python`
- `pandas` (for data manipulation)
- `transformers`

**Usage**:
```bash
python traffic_data_producer.py
python traffic_management_consumer.py
```

### 4. Healthcare Patient Monitoring System

**Description**: Monitors patient health data in real-time to predict potential health issues and suggest interventions.

**Scripts**:
- `patient_data_producer.py`: Simulates patient health metrics and sends them to Kafka.
- `health_monitoring_consumer.py`: Consumes health data, processes it, and uses the LLM to provide health recommendations.

**Dependencies**:
- `kafka-python`
- `numpy` (for numerical operations)
- `transformers`

**Usage**:
```bash
python patient_data_producer.py
python health_monitoring_consumer.py
```

### 5. Manufacturing Quality Control

**Description**: Improves product quality by analyzing data from IoT sensors on the production line.

**Scripts**:
- `manufacturing_data_producer.py`: Generates simulated IoT sensor data and publishes it to Kafka.
- `quality_control_consumer.py`: Consumes sensor data, processes it, and uses the LLM to identify quality issues.

**Dependencies**:
- `kafka-python`
- `matplotlib` (for data visualization)
- `transformers`

**Usage**:
```bash
python manufacturing_data_producer.py
python quality_control_consumer.py
```

## References

- [Apache Kafka Official Documentation](https://kafka.apache.org/documentation/)
- [Transformers Library by Hugging Face](https://huggingface.co/transformers/)

## License

This project is licensed under the MIT License. See the [LICENSE](LICENSE) file for details.

