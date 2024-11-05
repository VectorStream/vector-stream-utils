from flask import Flask, render_template_string, request, jsonify
import asyncio
from kafka_producer import KafkaProducer, products

app = Flask(__name__)

# Global variable to store the Kafka producer instance
kafka_producer = KafkaProducer()

# Reactive counter
clicks = 0

# HTML template for the app with Bootstrap
html_template = """
<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Ecommerce App</title>
    <link rel="stylesheet" href="https://stackpath.bootstrapcdn.com/bootstrap/4.5.2/css/bootstrap.min.css">
    <style>
      .animate-fadein {
            animation: fadein 1s;
        }
        @keyframes fadein {
            from { opacity: 0; }
            to   { opacity: 1; }
        }
    </style>
</head>
<body>
    <div class="container mt-5">
        <h1 class="text-center">Ecommerce App</h1>
        <button id="start-producer" class="btn btn-success" onclick="startProducer()">Start Producer</button>
        <button id="stop-producer" class="btn btn-danger" onclick="stopProducer()">Stop Producer</button>
        <h2 class="mt-4">Products</h2>
        <ul id="product-list" class="list-group">
            {% for product in products_list %}
            <li class="list-group-item">{{ product['name'] }} - {{ product['category'] }} - ${{ product['price'] }}</li>
            {% endfor %}
        </ul>
        <div id="kafka-topics" style="margin-top:20px;">
            <h2>Kafka Topics:</h2>
            <ul id="topic-list" class="list-group">
            </ul>
        </div>
    </div>

    <script src="https://cdn.jsdelivr.net/npm/jquery@3.5.1/dist/jquery.slim.min.js"></script>
    <script>
        let topicCount = 0;
        function increment() {
            fetch('/increment', { method: 'POST' })
              .then(response => response.json())
              .then(data => {
                    document.getElementById('increment-button').innerText = `Clicked: ${data.clicks}`;
                });
        }

        function startProducer() {
            fetch('/start_producer', { method: 'POST' });
            // Start getting Kafka topics
            getKafkaTopics();
        }

        function stopProducer() {
            fetch('/stop_producer', { method: 'POST' });
        }

        function getKafkaTopics() {
            setInterval(() => {
                fetch('/get_kafka_topics')
                  .then(response => response.json())
                  .then(data => {
                        if (data.topics.length > topicCount) {
                            const topicList = document.getElementById('topic-list');
                            data.topics.slice(topicCount).forEach(topic => {
                                const li = document.createElement('LI');
                                li.className = 'list-group-item animate-fadein';
                                li.appendChild(document.createTextNode(topic));
                                topicList.appendChild(li);
                            });
                            topicCount = data.topics.length;
                        }
                    });
            }, 1000); // Update every 1 second
        }
    </script>
</body>
</html>
"""

@app.route('/')
def index():
    global clicks
    return render_template_string(html_template, clicks=clicks, products_list=list(products.values()))

@app.route('/increment', methods=['POST'])
def increment():
    global clicks
    clicks += 1
    return jsonify({'clicks': clicks})

@app.route('/start_producer', methods=['POST'])
def start_producer():
    asyncio.run(kafka_producer.start())
    return '', 204

@app.route('/stop_producer', methods=['POST'])
def stop_producer():
    asyncio.run(kafka_producer.stop())
    return '', 204

@app.route('/get_kafka_topics', methods=['GET'])
def get_kafka_topics():
    # Simulate fetching Kafka topics (replace with actual logic)
    # For demonstration, let's assume we have a list of topics generated over time
    from kafka_producer import kafka_topics
    return jsonify({'topics': kafka_topics})

if __name__ == '__main__':
    app.run(debug=True)
