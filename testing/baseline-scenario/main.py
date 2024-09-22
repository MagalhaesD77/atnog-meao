import subprocess
import time
import json
import threading
import dateutil.parser as dp
from confluent_kafka import Consumer, KafkaError

threshold = (3/8)*100
memory_size = (8*pow(1024,3))
containerName = None
waiting_for_cAdvisor = False
namespace = "default"
command = "stress-ng --vm 1 --vm-bytes 4G"  # Infinite timeout
csvfile = "results.csv"  # CSV file to log times
deployment_file = "deployment.yaml"
time_string = ""

with open("results.csv", "w") as log_file:
    log_file.write("Metrics Collection,Target Pod Initialization,Target Pod Read" + "\n")

def log_to_csv():
    global time_string
    timestamps = [float(ts) for ts in time_string.split(',') if ts]
    differences = [0]
    for i in range(1, len(timestamps)):
        differences.append((timestamps[i] - timestamps[i - 1])* 1000)
    differences_str = ','.join(f"{diff:.6f}" for diff in differences)
    print(differences_str)
    with open("results.csv", "a") as log_file:
        log_file.write(differences_str + "\n")
    time_string = ""

def kubectl_apply():
    try:
        subprocess.run(
            ["kubectl", "apply", "-f", deployment_file],
            check=True
        )
        print("Applied deployment file.")
    except subprocess.CalledProcessError as e:
        print(f"Failed to apply deployment: {e.stderr.decode('utf-8') if e.stderr else 'No stderr output'}")

def kubectl_delete():
    try:
        subprocess.run(
            ["kubectl", "delete", "-f", deployment_file],
            check=True
        )
        print("Deleted deployment file.")
    except subprocess.CalledProcessError as e:
        print(f"Failed to delete deployment: {e.stderr.decode('utf-8') if e.stderr else 'No stderr output'}")

def get_pod_names():
    try:
        result = subprocess.run(
            ["kubectl", "get", "pods", "-n", namespace, "-o", "jsonpath={.items[*].metadata.name}"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True
        )
        pods = result.stdout.decode('utf-8').split()
        return pods
    except subprocess.CalledProcessError as e:
        print(f"Failed to get pods: {e.stderr.decode('utf-8') if e.stderr else 'No stderr output'}")
        return []

def get_pod_status(pod_name):
    try:
        result = subprocess.run(
            ["kubectl", "get", "pod", pod_name, "-n", namespace, "-o", "jsonpath={.status.phase}"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True
        )
        return result.stdout.decode('utf-8').strip()
    except subprocess.CalledProcessError as e:
        print(f"Failed to get pod status for {pod_name}: {e.stderr.decode('utf-8') if e.stderr else 'No stderr output'}")
        return None
    
def get_pod_container(pod_name):
    try:
        result = subprocess.run(
            ["kubectl", "get", "pod", pod_name, "-n", namespace, "-o", "jsonpath={.status.containerStatuses[].containerID}"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True
        )
        return result.stdout.decode('utf-8').replace("://", "-")
    except subprocess.CalledProcessError as e:
        print(f"Failed to get pod container for {pod_name}: {e.stderr.decode('utf-8') if e.stderr else 'No stderr output'}")
        return None

def processContainerMetrics(values):
    global threshold
    global memory_size

    # Memory
    memLoad = (values["container_stats"]["memory"]["usage"]/memory_size)*100
    
    if memLoad > threshold:
        return True
    else:
        return False

def consume_metrics():
    global time_string
    global containerName
    global waiting_for_cAdvisor
    kafka_consumer_conf = {
        'bootstrap.servers': '10.255.32.132:31999',
        'group.id': 'monitoring',
        'auto.offset.reset': 'latest'
    }
    metrics_collector_kafka_topic = 'k8s-cluster'

    # Create Kafka consumer
    consumer = Consumer(kafka_consumer_conf)

    # Subscribe to the topic
    consumer.subscribe([metrics_collector_kafka_topic])

    try:
        print("Listening to Kafka on topic {}....".format(metrics_collector_kafka_topic))
        while True:
            # Poll for messages
            message = consumer.poll(1.0)

            if message is None:
                continue
            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition
                    continue
                else:
                    # Error
                    print("Error: {}".format(message.error()))
                    break

            # Process the message
            values = json.loads(message.value().decode('utf-8'))
            cName = values["container_Name"]
            try:
                if containerName and containerName in cName:
                    if processContainerMetrics(values) and waiting_for_cAdvisor:
                        waiting_for_cAdvisor = False
                        containerName = None
                        dataCollectionTime = dp.parse(values["timestamp"]).timestamp()
                        time_string += str(dataCollectionTime) + ","
                        print(time_string)
            except RuntimeError as e:
                print("INFO: Array changed size during latency reading operation")

    except KeyboardInterrupt:
        # Stop consumer on keyboard interrupt
        consumer.close()

def exec_command(pod_name):
    try:
        subprocess.run(
            ["kubectl", "exec", "-n", namespace, "-it", pod_name, "--", "/bin/bash", "-c", command],
            check=True
        )
        print(f"Executed command on pod: {pod_name}")
    except subprocess.CalledProcessError as e:
        print(f"Failed to exec command on pod {pod_name}: {e.stderr.decode('utf-8') if e.stderr else 'No stderr output'}")

def monitor_new_pod(initial_pod, event):
    global time_string
    initial_pods = set(initial_pod)
    while not event.is_set():
        current_pods = set(get_pod_names())
        new_pods = current_pods - initial_pods
        if new_pods:
            new_pod_name = new_pods.pop()
            print(f"New pod detected: {new_pod_name}")
            time_string += str(time.time()) + ","
            pod_status = None
            while pod_status != "Running":
                pod_status = get_pod_status(new_pod_name)
                if pod_status == "Running":
                    time_string += str(time.time()) + ","
                    print(f"New pod {new_pod_name} is running")
                    log_to_csv()
                    event.set()
                    break

def wait_for_pods(desired_count):
    while True:
        pods = get_pod_names()
        if len(pods) == desired_count:
            break

def main():
    global waiting_for_cAdvisor
    global containerName
    # Start the kafka metrics consumer
    metrics_thread = threading.Thread(target=consume_metrics, args=())
    metrics_thread.start()

    while True:
        wait_for_pods(0)
        kubectl_apply()
        event = threading.Event()
        
        # Wait for the initial pod to be running
        initial_pod = None
        while not initial_pod:
            pods = get_pod_names()
            if pods:
                initial_pod = pods[0]
        
        initial_pod_status = None
        while initial_pod_status != "Running":
            initial_pod_status = get_pod_status(initial_pod)

        containerName = get_pod_container(initial_pod)
        
        print(f"Initial pod {initial_pod} is running.")

        # Start the infinite stress command
        stress_thread = threading.Thread(target=exec_command, args=(initial_pod,))
        stress_thread.start()

        waiting_for_cAdvisor = True
        while True:
            if not waiting_for_cAdvisor:
                print("done waiting")
                break

        # Start monitoring for the new pod
        monitor_thread = threading.Thread(target=monitor_new_pod, args=([initial_pod], event))
        monitor_thread.start()
        
        # Wait until the monitor thread sets the event
        event.wait()
        
        wait_for_pods(2)
        kubectl_delete()

if __name__ == "__main__":
    main()