import subprocess
import time
import logging
import threading
import csv

namespace = "default"
command = "stress-ng --vm 1 --vm-bytes 4G"  # Infinite timeout
csvfile = "results.csv"  # CSV file to log times
deployment_file = "deployment.yaml"
poll_interval = 1  # Poll interval in seconds
time_string = ""

with open("results.csv", "w") as log_file:
    log_file.write("Target Node Pod Initialization,Target Node Pod Read" + "\n")

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
        logging.info("Applied deployment file.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to apply deployment: {e.stderr.decode('utf-8') if e.stderr else 'No stderr output'}")

def kubectl_delete():
    try:
        subprocess.run(
            ["kubectl", "delete", "-f", deployment_file],
            check=True
        )
        logging.info("Deleted deployment file.")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to delete deployment: {e.stderr.decode('utf-8') if e.stderr else 'No stderr output'}")

def get_pod_names():
    try:
        result = subprocess.run(
            ["kubectl", "get", "pods", "-n", namespace, "-o", "jsonpath={.items[*].metadata.name}"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True
        )
        pods = result.stdout.decode('utf-8').split()
        return pods
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to get pods: {e.stderr.decode('utf-8') if e.stderr else 'No stderr output'}")
        return []

def get_pod_status(pod_name):
    try:
        result = subprocess.run(
            ["kubectl", "get", "pod", pod_name, "-n", namespace, "-o", "jsonpath={.status.phase}"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True
        )
        return result.stdout.decode('utf-8').strip()
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to get pod status for {pod_name}: {e.stderr.decode('utf-8') if e.stderr else 'No stderr output'}")
        return None

def exec_command(pod_name):
    try:
        subprocess.run(
            ["kubectl", "exec", "-n", namespace, "-it", pod_name, "--", "/bin/bash", "-c", command],
            check=True
        )
        logging.info(f"Executed command on pod: {pod_name}")
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to exec command on pod {pod_name}: {e.stderr.decode('utf-8') if e.stderr else 'No stderr output'}")

def monitor_new_pod(initial_pod, event):
    global time_string
    initial_pods = set(initial_pod)
    new_pod_appeared_time = None
    while not event.is_set():
        current_pods = set(get_pod_names())
        new_pods = current_pods - initial_pods
        if new_pods:
            new_pod_name = new_pods.pop()
            logging.info(f"New pod detected: {new_pod_name}")
            time_string += str(time.time()) + ","
            pod_status = None
            while pod_status != "Running":
                pod_status = get_pod_status(new_pod_name)
                if pod_status == "Running":
                    time_string += str(time.time()) + ","
                    logging.info(f"New pod {new_pod_name} is running")
                    log_to_csv()
                    event.set()
                    break
                time.sleep(poll_interval)
        time.sleep(poll_interval)

def wait_for_pods(desired_count):
    while True:
        pods = get_pod_names()
        if len(pods) == desired_count:
            break
        time.sleep(poll_interval)

def main():
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
            time.sleep(poll_interval)
        
        initial_pod_status = None
        while initial_pod_status != "Running":
            initial_pod_status = get_pod_status(initial_pod)
            time.sleep(poll_interval)
        
        logging.info(f"Initial pod {initial_pod} is running.")
        
        # Start the infinite stress command
        stress_thread = threading.Thread(target=exec_command, args=(initial_pod,))
        stress_thread.start()
        
        # Start monitoring for the new pod
        monitor_thread = threading.Thread(target=monitor_new_pod, args=([initial_pod], event))
        monitor_thread.start()
        
        # Wait until the monitor thread sets the event
        event.wait()
        
        wait_for_pods(2)
        kubectl_delete()

if __name__ == "__main__":
    main()