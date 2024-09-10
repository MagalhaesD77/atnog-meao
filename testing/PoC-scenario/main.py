import subprocess
import time
import logging

namespace = "10a613e3-46b4-433a-b836-eb15df1267cb"
command = "stress-ng --vm 1 --vm-bytes 4G"
logfile = "log.txt"  # Change to your desired log file path
stress_duration = 15  # Duration to run the stress-ng command

# Configure logging
logging.basicConfig(filename=logfile, level=logging.INFO, 
                    format='%(asctime)s - %(levelname)s - %(message)s')

def get_pod_name():
    try:
        result = subprocess.run(
            ["kubectl", "get", "pods", "-n", namespace, "-o", "jsonpath={.items[*].metadata.name}"],
            stdout=subprocess.PIPE, stderr=subprocess.PIPE, check=True
        )
        pods = result.stdout.decode('utf-8').split()
        print(result.stdout.decode('utf-8'))
        return pods
    except subprocess.CalledProcessError as e:
        logging.error(f"Failed to get pods: {e.stderr.decode('utf-8') if e.stderr else 'No stderr output'}")
        return None

def exec_command(pod_name):
    try:
        subprocess.run(
            ["kubectl", "exec", "-n", namespace, "-it", pod_name, "--", "/bin/bash", "-c", f"timeout {stress_duration} {command}"],
            check=True
        )
        logging.info(f"Executed command on pod: {pod_name}")
    except subprocess.CalledProcessError as e:
        if e.returncode == 124:
            logging.info(f"Command on pod {pod_name} timed out after {stress_duration} seconds")
        else:
            logging.error(f"Failed to exec command on pod {pod_name}: {e.stderr.decode('utf-8') if e.stderr else 'No stderr output'}")

def main():
    last_pod_name = None
    fail_count = 0

    while True:
        pods = get_pod_name()
        if not pods:
            fail_count += 1
            logging.warning(f"No pods found or error occurred. Fail count: {fail_count}")
        elif len(pods) == 1 and pods[0] != last_pod_name:
            exec_command(pods[0])
            last_pod_name = pods[0]
            fail_count = 0  # Reset fail count on success
        else:
            logging.info(f"Pod check: {pods}, last pod: {last_pod_name}")

        if fail_count >= 5:
            logging.error("Failed 5 times, exiting.")
            break
        
        time.sleep(30)

if __name__ == "__main__":
    main()