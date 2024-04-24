import requests
import json
from time import sleep
from confluent_kafka import Consumer, KafkaError
import warnings
import threading
import subprocess
import os

def migrationAlgorithm(cpuLoad, memLoad):
    if cpuLoad > cpuLoadThresh or memLoad > memLoadThresh:
        return True
    return False

def migrate(authToken, container, migrate):
    if migrate:            
        returnMsg = callEndpoints(
            authToken, 
            "/nslcm/v1/ns_instances/{}/migrate_k8s".format(container["ns_id"]), 
            "POST", 
            data = json.dumps({
                "vnfInstanceId": container["vnf_id"],
                "migrateToHost": "k3s-controller-pedrocjdpereira",
                "kdu": {
                    "kduId": container["kdu_id"],
                    "kduCountIndex": 0,
                }
            })
        )
        print("MIGRATE CONTAINER", container)
        print("returnMsg:", returnMsg)

def collectMetrics(cName, values, previousCPU, previousSystem):
    #print(json.dumps(values, indent=2))

    print("-------------------------------------------------------")
    print("Container Name:", cName)
    print("Timestamp:", values["timestamp"])


    # Memory
    memUsage = values["container_stats"]["memory"]["usage"]
    #print("Memory Usage:", memUsage)
    memLoad = (memUsage/(ram*pow(1024,3))) * 100
    print("Memory Load:", memLoad)


    # CPU
    timestampParts = timestampParts = values["timestamp"].split(':')
    timestamp = (float(timestampParts[-3][-2:])*pow(60,2) + float(timestampParts[-2])*60 + float(timestampParts[-1][:-1])) * pow(10, 9)
    currentCPU = values["container_stats"]["cpu"]["usage"]["total"]
    cpuLoad = 0
    if previousCPU != 0 and previousSystem != 0:
        # Calculate CPU usage delta
        cpuDelta = currentCPU - previousCPU
        #print("CPU Delta: ", cpuDelta)

        # Calculate System delta
        systemDelta = timestamp - previousSystem
        #print("System Delta", systemDelta)

        if systemDelta > 0.0 and cpuDelta >= 0.0:
            cpuLoad = ((cpuDelta / systemDelta) / numCores) * 100
            print("CPU Load:", cpuLoad)
    previousCPU = currentCPU
    previousSystem = timestamp

    #print("Network RX Bytes:", values["container_stats"]["network"]["rx_bytes"])
    #print("Network TX Bytes:", values["container_stats"]["network"]["tx_bytes"])
    #if values["container_stats"]["diskio"] != {}:
        #print("Disk IO Read:", values["container_stats"]["diskio"]["io_service_bytes"][0]["stats"]["Read"])
        #print("Disk IO Write:", values["container_stats"]["diskio"]["io_service_bytes"][0]["stats"]["Write"])
    print("-------------------------------------------------------")

    metrics = {
        "cpuLoad": cpuLoad,
        "memLoad": memLoad,
        "previousCPU": previousCPU,
        "previousSystem": previousSystem
    }

    return metrics

def getAuthToken():
    # Authentication
    endpoint = NBI_URL + '/admin/v1/tokens'
    result = {'error': True, 'data': ''}
    headers = {"Content-Type": "application/yaml", "accept": "application/json"}
    data = {"username": 'admin', "password": 'admin'}
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning)
            r = requests.post(endpoint, headers=headers,  json=data, verify=False)
    except Exception as e:
        result["data"] = str(e)
        return result

    if r.status_code == requests.codes.ok:
        result['error'] = False

    result["data"] = r.text
    token = json.loads(r.text)
    return token

def callEndpoints(authToken, endpoint, method, data=None):
    # Get NS Instances
    endpoint = NBI_URL + endpoint
    result = {'error': True, 'data': ''}
    headers = {"Content-Type": "application/yaml", "accept": "application/json",
            'Authorization': 'Bearer {}'.format(authToken["id"])}
    try:
        with warnings.catch_warnings():
            warnings.filterwarnings("ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning)
            match method:
                case "GET":
                    r = requests.get(endpoint, data=data, params=None, verify=False, stream=True, headers=headers)
                case "POST":
                    r = requests.post(endpoint, data=data, params=None, verify=False, stream=True, headers=headers)
    except Exception as e:
        result['data'] = str(e)
        return result

    if r.status_code == requests.codes.ok:
        result['error'] = False

    result['data'] = r.text
    info = r.text

    return info

def getContainerInfo(authToken):
    #'  - containerID: containerd://181a7e50d8760653cd7583ee1801811d6f2f96b1848c219db49c43c52e994e98\n' +
    ns_instances = callEndpoints(authToken, "/nslcm/v1/ns_instances", "GET")
    try:
        ns_instances = json.loads(ns_instances)
    except Exception as e:
        print(e)
    
    if len(ns_instances) < 1:
        print('ERROR: No deployed ns instances')
    elif 'code' in ns_instances[0].keys():
        print('ERROR: Error calling ns_instances endpoint')

    containerInfo = []

    for ns_instance in ns_instances:
        ns_id = ns_instance["_id"]
        vnf_ids = ns_instance["constituent-vnfr-ref"]
        vnf_instances = {}
        for vnf_id in vnf_ids:
            vnfContent = callEndpoints(authToken, "/nslcm/v1/vnf_instances/{}".format(vnf_id), "GET")
            try:
                vnfContent = json.loads(vnfContent)
            except Exception as e:
                print(e)
            vnf_instances[vnfContent["member-vnf-index-ref"]] = vnfContent["_id"]
        kdu_instances = ns_instance["_admin"]["deployed"]["K8s"]
        for kdu in kdu_instances:
            kdu_instance = kdu["kdu-instance"]
            member_vnf_index = kdu["member-vnf-index"]
            namespace = kdu["namespace"]
            vnf_id = vnf_instances[member_vnf_index]

            command = (
                "{} --kubeconfig={} --namespace={} get pods -l ns_id={} -o=json".format(
                    kubectl_command,
                    os.path.join(os.environ['HOME'], kube_config_path),
                    namespace,
                    ns_id,
                )
            )
            try:
                # Execute the kubectl command and capture the output
                k8s_info = json.loads(subprocess.check_output(command.split()))
            except subprocess.CalledProcessError as e:
                # Handle any errors if the command fails
                print("Error executing kubectl command:", e)
                return None

            for pod in k8s_info["items"]:
                nodeName = pod["spec"]["nodeName"]
                containers = pod["status"]["containerStatuses"]
                for container in containers:
                    if "containerID" in container:
                        id = container["containerID"]
                        containerInfo.append({
                            "id": id.strip('"').split('/')[-1],
                            "ns_id": ns_id,
                            "vnf_id": vnf_id,
                            "kdu_id": kdu_instance,
                            "node": nodeName,
                            }
                        )

    return containerInfo

def consume_messages(authToken):
    global containerInfo

    # Create Kafka consumer
    consumer = Consumer(consumer_conf)

    # Subscribe to the topic
    consumer.subscribe([topic])

    try:
        
        metrics = {
            "cpuLoad": 0,
            "memLoad": 0,
            "previousCPU": 0,
            "previousSystem": 0
        }
        
        print("Listening to Kafka....")
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
            for container in containerInfo:
                if container["id"] in cName:
                    metrics = collectMetrics(cName, values, metrics["previousCPU"], metrics["previousSystem"])

                    res = migrationAlgorithm(metrics["cpuLoad"], metrics["memLoad"])
                    migrate(authToken, container, res)

    except KeyboardInterrupt:
        # Stop consumer on keyboard interrupt
        consumer.close()

def update_container_ids(authToken):
    global containerInfo
    while True:
        containerInfo = getContainerInfo(authToken)
        print("Container Info: " + str(containerInfo))
        sleep(5)  # Wait for 5 seconds before updating again

if __name__ == '__main__':
    authToken = getAuthToken()
    # Create threads
    consume_thread = threading.Thread(target=consume_messages, args=(authToken,))
    update_thread = threading.Thread(target=update_container_ids, args=(authToken,))

    # Start threads
    consume_thread.start()
    update_thread.start()
