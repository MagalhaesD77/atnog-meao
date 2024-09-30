import time
import threading
import os
import json
from flask import Flask, request, jsonify, render_template_string, redirect
from confluent_kafka import Producer

# Kafka configuration
conf = {'bootstrap.servers': "10.255.32.132:31999"}
producer = Producer(conf)
topic = "ue-lat"

# Flask app
app = Flask(__name__)

# List of K3s node names
k3s_nodes = ["k3s-worker1-pedrocjdpereira", "k3s-worker2-pedrocjdpereira", "k3s-controller-pedrocjdpereira"]

# Initial latencies and target latencies
latencies = {
    "k3s-worker1-pedrocjdpereira": 10,
    "k3s-worker2-pedrocjdpereira": 10,
    "k3s-controller-pedrocjdpereira": 1000,
}
target_latencies = latencies.copy()

@app.route('/dashboard', methods=['GET'])
def dashboard():
    global latencies
    html = '''
    <!DOCTYPE html>
    <html>
    <head>
        <title>Update Latencies</title>
    </head>
    <body>
        <h1>Update UE Latencies</h1>
        <form method="post" action="/update">
            {% for node, latency in latencies.items() %}
                <label for="{{ node }}">Latency to {{ node }} (current: {{ latency }}):</label>
                <input type="number" id="{{ node }}" name="{{ node }}" step="1"><br><br>
            {% endfor %}
            <input type="submit" value="Update">
        </form>
    </body>
    </html>
    '''
    return render_template_string(html, latencies=latencies)

@app.route('/update', methods=['POST'])
def update_latencies():
    data = request.form
    print(data)
    for node in target_latencies:
        if node in data:
            try:
                target_latencies[node] = int(data[node])
            except Exception as e:
                continue
    return redirect('/dashboard')

def delivery_report(err, msg):
    """ Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush(). """
    if err is not None:
        print('Message delivery failed: {}'.format(err))
    else:
        print('Message delivered to {} - {}'.format(msg.topic(), msg.value()))

def adjust_distance(current, target, step=2):
    if current < target:
        return min(current + step, target)
    elif current > target:
        return max(current - step, target)
    return current

def produce_messages():
    while True:
        global latencies
        for node in latencies:
            latencies[node] = adjust_distance(latencies[node], target_latencies[node])
        message = json.dumps(latencies)
        producer.produce(topic, key='latencies', value=message, callback=delivery_report)
        producer.poll(0)
        time.sleep(1)

if __name__ == '__main__':
    # Start producer thread
    producer_thread = threading.Thread(target=produce_messages)
    producer_thread.daemon = True
    producer_thread.start()

    # Start Flask app
    app.run(host='0.0.0.0', port=5000)
