import json
import asyncio
import time
import threading
import time
import dateutil.parser as dp
from confluent_kafka import Consumer, KafkaError

class MEAO:
    """
    This class represents the MEAO monitoring and migration agent

    ...

    Attributes
    ----------
    nbi_k8s_connector : NBIConnector
        instance of NBIConnector which simplifies interactions with OSM's NBI and the Kubernetes API
    update_container_ids_freq : int
        length of time (in seconds) that the update_container_ids must wait between each update
    metrics_collector_kafka_topic : str
        kafka topic for the MEAO to subscribe to consume metrics information relating to the containers
    ue_latency_kafka_topic : str
        kafka topic for the MEAO to subscribe to consume latency information relating to the containers
    kafka_consumer_conf: str
        kafka consumer configuration (IP, offset, etc.)
    migratingContainers: dict
        dictionary to monitor which containers are currently being migrated,
        mapping the container's ID to the corresponding OSM migration operation ID
    cpu_history: dict
        dictionary utilized for cpuLoad calculations
    nodeSpecs: dict
        dictionary storing information relating to the cluster's nodes,
        mapping the node's name to the corresponding node information:
            - num_cpu_cores: int
                the number of CPU cores of the node
            - memory_size: float
                the amount of RAM of the node in GBs
            - cadvisor: str
                name of the cadvisor pod that is collecting metrics in the node
            - cpuLoad: float
                the node's CPU load as calculated based on the collected metrics
            - memLoad: float
                the node's memory load as calculated based on the collected metrics
    containerInfo: dict
        dictionary storing information relating to OSM-deployed containers,
        mapping the container's ID to the corresponding container information:
            - ns_id: str
                the ID of the associated Network Service (NS)
            - vnf_id: str
                the ID of the associated Virtual Network Function (VNF)
            - kdu_id: str
                the ID of the associated Kubernetes Deployment Unit (KDU)
            - node: str
                the node in which the container is deployed
            - cpuLoad: float
                the container's CPU load on the node as calculated based on the collected metrics
            - memLoad: float
                the container's memory load on the node as calculated based on the collected metrics
            - ue-lats: dict
                dictionary storing latency information
            - migration_policy: dict
                dictionary containing the container's migration policy, if stipulated:
                    - mem_load_thresh: float
                        the memory load of the container on the node that corresponds to the "allocated-mem" value
                    - mem_surge_capacity: float
                        the memory load of the container on the node that corresponds to the "mem-surge-capacity" value
                    - cpu_load_thresh: float
                        the cpu load of the container on the node that corresponds to the "allocated-cpu" value
                    - cpu_surge_capacity: float
                        the cpu load of the container on the node that corresponds to the "cpu-surge-capacity" value
                    - mobility-migration-factor: float
                        used for determining migration based on latency information
    """

    def __init__(self, nbi_k8s_connector, update_container_ids_freq, metrics_collector_kafka_topic, ue_latency_kafka_topic, kafka_consumer_conf) -> None:
        self.nbi_k8s_connector = nbi_k8s_connector
        self.update_container_ids_freq = update_container_ids_freq
        self.metrics_collector_kafka_topic = metrics_collector_kafka_topic
        self.ue_latency_kafka_topic = ue_latency_kafka_topic
        self.kafka_consumer_conf = kafka_consumer_conf
        self.migratingContainers = {}
        self.cpu_history = {}
        self.nodeSpecs = self.nbi_k8s_connector.getNodeSpecs()
        print("Node Specs: " + str(self.nodeSpecs))
        self.containerInfo = self.nbi_k8s_connector.getContainerInfo(self.nodeSpecs)
        print("Container Info: " + str(self.containerInfo))
        self.log = {}
        with open("results.csv", "w") as log_file:
            log_file.write("Metrics Collection,Metrics Reception,Migration Decision,Target Pod Initialization,Target Pod Ready,Source Pod Termination,Migration Completion in OSM\n")

    def start(self):
        """
        Starts all threads:

            read_metrics_collector:
                thread for collecting and processing metrics relating to each node and container
                if necessary, a migration operation will be scheduled

            read_ue_latency:
                thread for collecting and processing latency information
                if necessary, a migration operation will be scheduled

            update_container_ids:
                thread for updating the nodeSpecs, containerInfo and migrationContainers dictionary

            watch_kubectl_thread:
                thread for testing purposes
        """
        
        # Create threads
        read_metrics_collector = threading.Thread(target=self.read_metrics_collector)
        read_ue_latency = threading.Thread(target=self.read_ue_latency)
        update_thread = threading.Thread(target=self.update_container_ids)
        watch_kubectl_thread = threading.Thread(target=self.watch_kubectl)

        # Start threads
        read_metrics_collector.start()
        read_ue_latency.start()
        update_thread.start()
        watch_kubectl_thread.start()

    def get_node_specs(self, hostname=None):
        """
        Returns information relating to the cluster's nodes

        Parameters
        ----------
        hostname : str, optional
            if specified, the function returns only the chosen node's information (default is None)
        """
        if hostname:
            if hostname in self.nodeSpecs.keys():
                return self.nodeSpecs[hostname]
            else:
                return None
        else:
            return self.nodeSpecs

    def get_container_ids(self):
        return self.containerInfo

    def update_node_specs(self):
        self.nodeSpecs = self.nbi_k8s_connector.getNodeSpecs()

    def min_usage_node(self):
        """
        Returns the name and usage of the node with the least resource usage (cpu and memory) 
        """
        min_usage_node = None
        min_usage = 0
        for node, nodeInfo in self.nodeSpecs.items():
            if "cpuLoad" not in nodeInfo or "memLoad" not in nodeInfo:
                continue
            usage = nodeInfo["cpuLoad"] + nodeInfo["memLoad"]
            if not min_usage_node or usage < min_usage:
                min_usage_node = node
                min_usage = usage
        return min_usage_node, min_usage

    def min_lat_MEH(self, ue_lats):
        """
        Returns the name and latency of the node with the least latency

        Parameters
        ----------
        ue_lats : dict
            latency information
        """
        min_lat_meh = None
        min_lat = 0
        for meh, meh_lat in ue_lats.items():
            if not min_lat_meh or meh_lat < min_lat:
                min_lat_meh = meh
                min_lat = meh_lat
        return min_lat_meh, min_lat
    
    def resourceMigrationAlgorithm(self, container, cName):
        """
        Processes the CPU and memory load of the container on the node and determines whether a migration operation must be scheduled

        The migration logic can be resumed in the following manner:

            If the current node cpu/memLoad + the container's cpu/mem_surge_capacity is larger than 100:
                - the container must be migrated since the node does not have the conditions for it
            If the current container cpu/memLoad on the node is larger than cpu/mem_load_thresh + cpu/mem_surge_capacity:
                - the container must be migrated since the Service-Level Agreement has been violated

        If the conditions for migration are verified, the function returns the name of the node with the least resource usage, else it returns None

        Parameters
        ----------
        container : dict
            information relating to the container whose metrics are being processed, obtained from the containerInfo dictionary
        cName : str
            the container's ID
        """
        cpuLoad = container["cpuLoad"]
        memLoad = container["memLoad"]
        node_cpuLoad = self.nodeSpecs[container["node"]]["cpuLoad"]
        node_memLoad = self.nodeSpecs[container["node"]]["memLoad"]
        node_usage = node_cpuLoad + node_memLoad

        min_cpu_resources = container["migration_policy"]["cpu_load_thresh"]
        extra_cpu_resources = container["migration_policy"]["cpu_surge_capacity"]
        max_cpu_resources = min_cpu_resources + extra_cpu_resources

        min_mem_resources = container["migration_policy"]["mem_load_thresh"]
        extra_mem_resources = container["migration_policy"]["mem_surge_capacity"]
        max_mem_resources = min_mem_resources + extra_mem_resources

        if (
            (cpuLoad > max_cpu_resources) 
            or (memLoad > max_mem_resources)
            or (100-node_cpuLoad) < min(extra_cpu_resources, max_cpu_resources-cpuLoad)
            or (100-node_memLoad) < min(extra_mem_resources, max_mem_resources-memLoad)
        ):
            min_usage_node, min_usage = self.min_usage_node()
            if min_usage_node != container["node"] and min_usage < node_usage and cName not in self.migratingContainers:
                if cName in self.log.keys():
                    self.log[cName] += str(time.time()) + ","
                return min_usage_node
        
        if cName in self.log and cName not in self.migratingContainers:
            self.log.pop(cName)
        return None
        
    def latencyMigrationAlgorithm(self, container):
        """
        Processes latency information and determines whether a migration operation must be scheduled

        The migration logic can be resumed in the following manner:

            If a node is found where its latency is less than the current node's latency multiplied by the mobility migration factor:
                - the container must be migrated since the new node offers better conditions

        If the conditions for migration are verified, the function returns the name of the node with the least latency, else it returns None

        Parameters
        ----------
        container : dict
            information relating to the container whose metrics are being processed, obtained from the containerInfo dictionary
        """
        ue_lats = container["ue-lats"]

        current_meh = container["node"]
        current_meh_lat = ue_lats[current_meh]

        min_lat_meh, min_lat = self.min_lat_MEH(ue_lats)

        if min_lat_meh != current_meh and min_lat < container["migration_policy"]["mobility-migration-factor"]*current_meh_lat:
            return min_lat_meh
        
        return None

    def migrationAlgorithm(self, cName):
        """
        Obtains the results from both the resource and latency migration algorithms and determines whether a migration operation must be scheduled

        If both algorithms decided that migration was necessary, specifiying different target nodes, a final target must be decided
        The logic for this decision can be resumed in the following manner:

            If the latency of the target node chosen by the resource migration algorithm is whithin a reasonable range
            (less than the latency of the node chosen by the latency migration algorithm divided by the mobility migration factor):
                - the final target node is the one chosen by the resource migration algorithm, since it offers better conditions overall
            If not, then:
                - the final target node is the one chosen by the latency migration algorithm, since latency must be prioritized

        If the conditions for migration are verified, the function schedules the migration operation in OSM 
        and updates the migratingContainers dictionary accordingly

        Parameters
        ----------
        cName : str
            the container's ID
        """
        container = self.containerInfo[cName]
        currentNode = container["node"]
        if (
            not container["migration_policy"] 
            or cName in self.migratingContainers
            or "cpuLoad" not in self.nodeSpecs[currentNode]
            or "memLoad" not in self.nodeSpecs[currentNode]
        ):
            return None
        
        # obtain the results from both the resource and latency migration algorithms
        resourceTargetNode = None
        latencyTargetNode = None

        if (
            (container["migration_policy"]["cpu_load_thresh"] and "cpuLoad" in container)
            or (container["migration_policy"]["mem_load_thresh"] and "memLoad" in container)
        ):
            resourceTargetNode = self.resourceMigrationAlgorithm(container, cName)
        if container["migration_policy"]["mobility-migration-factor"] and "ue-lats" in container:
            latencyTargetNode = self.latencyMigrationAlgorithm(container)

        # if the conditions demand the migration of the container, determine the final migration target, else return None
        finalTargetNode = None
        # if both algorithms decided that migration was necessary, the final target must be decided
        if (
            (resourceTargetNode and resourceTargetNode in self.nodeSpecs.keys())
            and (latencyTargetNode and latencyTargetNode in self.nodeSpecs.keys())
        ):
            # if both algorithms specified the same target node, there is no issue
            if resourceTargetNode == latencyTargetNode or container["migration_policy"]["mobility-migration-factor"] == 0:
                finalTargetNode = resourceTargetNode
            # if not, a final target node must be chosen 
            else:
                latency_meh_lat = container["ue-lats"][latencyTargetNode]
                resource_meh_lat = container["ue-lats"][resourceTargetNode]
                # the target node chosen by the resource migration algorithm is chosen, if its latency is whithin a reasonable range
                # (less than the latency of the node chosen by the latency migration algorithm divided by the mobility migration factor)
                if resource_meh_lat < latency_meh_lat/container["migration_policy"]["mobility-migration-factor"]:
                    finalTargetNode = resourceTargetNode
                # if not, latency is prioritized and the final target node is the one chosen by the latency migration algorithm
                else:
                    finalTargetNode = latencyTargetNode
        # there is also no issue if only one of the algorithms decided that migration was necessary
        elif resourceTargetNode and resourceTargetNode in self.nodeSpecs.keys():
            finalTargetNode = resourceTargetNode
        elif latencyTargetNode and latencyTargetNode in self.nodeSpecs.keys():
            finalTargetNode = latencyTargetNode

        # schedule the migration operation and update the migratingContainers dictionary accordingly
        if finalTargetNode and cName not in self.migratingContainers:
            self.migratingContainers[cName] = "MIGRATING"
            op_id = self.nbi_k8s_connector.migrate(cName, container, finalTargetNode)
            if op_id:
                self.migratingContainers[cName] = op_id

    def processContainerMetrics(self, cName, values, container=None):
        """
        Processes the metrics information relating to a node or container and subsequently runs the migration algorithm

        Parameters
        ----------
        cName : str
            the container's ID
        values: dict
            dictionary containing metrics information relating to a node or container
        container: dict, optional
            information relating to the container whose metrics are being processed, obtained from the containerInfo dictionary (default is None)
        """
        metrics = self.calcMetrics(cName, values, container)

        if not container:
            self.nodeSpecs[cName]["cpuLoad"] = metrics["cpuLoad"]
            self.nodeSpecs[cName]["memLoad"] = metrics["memLoad"]
        else:        
            self.containerInfo[cName]["cpuLoad"] = metrics["cpuLoad"]
            self.containerInfo[cName]["memLoad"] = metrics["memLoad"]
            self.migrationAlgorithm(cName)
    
    def processContainerLatencies(self, cName, values):
        """
        Processes latency information and subsequently runs the migration algorithm

        Parameters
        ----------
        cName : str
            the container's ID
        values: dict
            dictionary containing latency information
        """
        self.containerInfo[cName]["ue-lats"] = values
        self.migrationAlgorithm(cName)

    def calcMetrics(self, cName, values, container=None, silent=True):
        """
        Calculates the cpu and memory load based on the metrics information relating to a node or container

        Parameters
        ----------
        cName : str
            the container's ID
        values: dict
            dictionary containing metrics information relating to a node or container
        container: dict, optional
            information relating to the container whose metrics are being processed, obtained from the containerInfo dictionary (default is None)
        silent: boolean, optional
            flag determining whether the information printing functionality is suppressed (default is True)
        """
        if container:
            memory_size = (self.nodeSpecs[container["node"]]["memory_size"]*pow(1024,3))
            num_cpu_cores = self.nodeSpecs[container["node"]]["num_cpu_cores"]
        else:
            memory_size = (self.nodeSpecs[cName]["memory_size"]*pow(1024,3))
            num_cpu_cores = self.nodeSpecs[cName]["num_cpu_cores"]

        # CPU load corresponds to the amount of time that was spent executing tasks over a certain time period
        # this can be obtained by calculating the ratio of CPU time delta over the system time delta

        # the current timestamp corresponds to the current system time
        timestampParts = values["timestamp"].split(':')
        timestamp = (float(timestampParts[-3][-2:])*pow(60,2) + float(timestampParts[-2])*60 + float(timestampParts[-1][:-1])) * pow(10, 9)
        # the cpu usage value corresponds to the current CPU time
        currentCPU = values["container_stats"]["cpu"]["usage"]["total"]
        if cName not in self.cpu_history.keys():
            self.cpu_history[cName] = {}
            self.cpu_history[cName]["previousCPU"] = 0
            self.cpu_history[cName]["previousSystem"] = 0

        cpuLoad = 0
        if self.cpu_history[cName]["previousCPU"] != 0 and self.cpu_history[cName]["previousSystem"] != 0:
            # calculate CPU usage delta
            cpuDelta = currentCPU - self.cpu_history[cName]["previousCPU"]
            # calculate System delta
            systemDelta = timestamp - self.cpu_history[cName]["previousSystem"]
            if systemDelta > 0.0 and cpuDelta >= 0.0:
                # calculate the ratio of the CPU time delta over the system time delta compared to the number of cores of the node
                cpuLoad = ((cpuDelta / systemDelta) / num_cpu_cores) * 100
                # bug -> should be investigated
                if cpuLoad > 100:
                    cpuLoad = 0
        # store the current times in the cpu_history dictionary for future iterations of this function
        self.cpu_history[cName]["previousCPU"] = currentCPU
        self.cpu_history[cName]["previousSystem"] = timestamp

        # memory load corresponds to the amount of memory used compared to the total memory of the node
        memUsage = values["container_stats"]["memory"]["usage"]
        memLoad = (memUsage/memory_size) * 100

        # for future use
        #print("Network RX Bytes:", values["container_stats"]["network"]["rx_bytes"])
        #print("Network TX Bytes:", values["container_stats"]["network"]["tx_bytes"])
        #if values["container_stats"]["diskio"] != {}:
            #print("Disk IO Read:", values["container_stats"]["diskio"]["io_service_bytes"][0]["stats"]["Read"])
            #print("Disk IO Write:", values["container_stats"]["diskio"]["io_service_bytes"][0]["stats"]["Write"])

        if not silent:
            print("-------------------------------------------------------")
            #print(json.dumps(values, indent=2))
            print("Container ID:", cName)
            print("Machine Name:", values["machine_name"])
            print("Timestamp:", values["timestamp"])
            print("CPU Load:", cpuLoad)
            print("Memory Load:", memLoad)
            print("-------------------------------------------------------")

        return {
            "cpuLoad": cpuLoad,
            "memLoad": memLoad,
        }

    def updateDict(self, oldDict, updatedDict):
        """
        Simple function to ensures the old dictionary is updated without altering the existing contents unnecessarily.

        Parameters
        ----------
        oldDict : dict
            dictionary to be updated
        values: dict
            dictionary with new content
        """
        keys_to_keep_nodeSpecs = set(oldDict.keys()).intersection(updatedDict.keys())
        keys_to_remove_nodeSpecs = set(oldDict.keys()) - keys_to_keep_nodeSpecs
        for key in keys_to_remove_nodeSpecs:
            oldDict.pop(key)
        
        keys_to_add_nodeSpecs = set(updatedDict.keys()) - set(oldDict.keys())
        for key in keys_to_add_nodeSpecs:
            oldDict[key] = updatedDict[key]

        return oldDict

    def read_metrics_collector(self):
        """
        !!! THREAD !!! 
        
        read_metrics_collector thread:
            thread for collecting and processing metrics relating to each node and container
            if necessary, a migration operation will be scheduled
        """
        # create kafka consumer
        consumer = Consumer(self.kafka_consumer_conf)

        # subscribe to the topic
        consumer.subscribe([self.metrics_collector_kafka_topic])

        try:
            print("Listening to Kafka on topic {}....".format(self.metrics_collector_kafka_topic))
            while True:
                
                # poll for messages
                message = consumer.poll(1.0)

                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        # end of partition
                        continue
                    else:
                        # error
                        print("Error: {}".format(message.error()))
                        break

                # process the message
                values = json.loads(message.value().decode('utf-8'))
                cName = values["container_Name"]
                # if the received information is related to a node
                if cName == "/":
                    # obtain the name of the associated cadvisor pod
                    machine_name = values["machine_name"]
                    # ensure nodeSpecs contains information relating to this node
                    if not any(machine_name in nodeInfo.values() for node, nodeInfo in self.nodeSpecs.items()):
                        continue
                    # if so, process the metrics information received
                    for node, nodeInfo in self.nodeSpecs.items():
                        if nodeInfo["cadvisor"] == machine_name:
                            t = threading.Thread(target=self.processContainerMetrics, args=(node, values,))
                            t.start()
                            break
                # if the received information is related to a container
                try:
                    for containerName, container in self.containerInfo.items():
                        # if it is a OSM-deployed container and containerInfo contains information
                        # relating to it, process the metrics information received
                        if containerName in cName:
                            if containerName not in self.migratingContainers:
                                dataCollectionTime = dp.parse(values["timestamp"]).timestamp()
                                self.log[containerName] = str(dataCollectionTime) + ","
                                self.log[containerName] += str(time.time()) + ","
                            t = threading.Thread(target=self.processContainerMetrics, args=(containerName, values, container,))
                            t.start()
                except RuntimeError as e:
                    print("INFO: Exception while processing kafka messages: ", e)

        except KeyboardInterrupt:
            # Stop consumer on keyboard interrupt
            consumer.close()

    def read_ue_latency(self):
        """
        !!! THREAD !!! 
        
        read_ue_latency:
            thread for collecting and processing latency information
            if necessary, a migration operation will be scheduled
        """
        # create kafka consumer
        consumer = Consumer(self.kafka_consumer_conf)

        # subscribe to the topic
        consumer.subscribe([self.ue_latency_kafka_topic])

        try:
            print("Listening to Kafka on topic {}....".format(self.ue_latency_kafka_topic))
            while True:
                
                # poll for messages
                message = consumer.poll(1.0)

                if message is None:
                    continue
                if message.error():
                    if message.error().code() == KafkaError._PARTITION_EOF:
                        # end of partition
                        continue
                    else:
                        # error
                        print("Error: {}".format(message.error()))
                        break

                # process the message
                values = json.loads(message.value().decode('utf-8'))
                try:
                    # since information regarding every node is received, each container's latency information 
                    # must be processed to decide whether a migration operation must be scheduled
                    for containerName in self.containerInfo.keys():
                        t = threading.Thread(target=self.processContainerLatencies, args=(containerName, values,))
                        t.start()
                except RuntimeError as e:
                    print("INFO: Exception while processing kafka messages: ", e)

        except KeyboardInterrupt:
            # Stop consumer on keyboard interrupt
            consumer.close()

    def update_container_ids(self):
        """
        !!! THREAD !!! 
        
        update_container_ids:
            thread for updating the nodeSpecs, containerInfo and migrationContainers dictionary
        """
        while True:
            time.sleep(self.update_container_ids_freq)

            # update node specs
            newNodeSpecs = self.nbi_k8s_connector.getNodeSpecs()
            for nodeName in self.nodeSpecs.keys():
                if (
                    nodeName in newNodeSpecs
                    and "cadvisor" in self.nodeSpecs[nodeName].keys() 
                    and "cadvisor" in newNodeSpecs[nodeName].keys() 
                    and self.nodeSpecs[nodeName]["cadvisor"] != newNodeSpecs[nodeName]["cadvisor"]
                ):
                    self.nodeSpecs[nodeName]["cadvisor"] = newNodeSpecs[nodeName]["cadvisor"]
            self.nodeSpecs = self.updateDict(self.nodeSpecs, newNodeSpecs)

            # update container info
            self.containerInfo = self.updateDict(self.containerInfo, self.nbi_k8s_connector.getContainerInfo(self.nodeSpecs))
            
            # update migrating containers
            idsToDelete = [
                container_id 
                for container_id, op_id in self.migratingContainers.items() 
                if op_id != "MIGRATING" and self.nbi_k8s_connector.getOperationState(op_id) != "PROCESSING"
            ]
            for container_id in idsToDelete:
                self.migratingContainers.pop(container_id)

            print("Container Info: " + json.dumps(self.containerInfo, indent=2))
            print("Node Specs: " + json.dumps(self.nodeSpecs, indent=2))

    def watch_kubectl(self):
        """
        !!! THREAD !!! 
        
        watch_kubectl_thread:
            thread for testing purposes
        """
        namespace = "17d7aed1-869a-4ced-b492-c3a8b0a433c2"
        old_pod_status = self.nbi_k8s_connector.get_pod_status(namespace)
        
        while True:
            new_pod_status = self.nbi_k8s_connector.get_pod_status(namespace)

            # check for new pods
            for pod_name, status in new_pod_status.items():
                if pod_name not in old_pod_status and len(list(self.migratingContainers.keys())) == 1:
                    # wait for it to get to "running" state
                    cName = list(self.migratingContainers.keys())[0]
                    if cName not in self.log.keys():
                        break
                    self.log[cName] += str(time.time()) + ","
                    while True:
                        new_pod_status = self.nbi_k8s_connector.get_pod_status(namespace)
                        if new_pod_status[pod_name] == "Running":
                            self.log[cName] += str(time.time()) + ","
                            break
                    
                    for pod, status in new_pod_status.items():
                        if pod in old_pod_status:
                            pod_name = pod
                            break

                    # wait for old one to be deleted
                    while True:
                        new_pod_status = self.nbi_k8s_connector.get_pod_status(namespace)
                        if pod_name not in new_pod_status.keys():
                            self.log[cName] += str(time.time()) + ","
                            break
                    
                    op_id = self.migratingContainers[cName]
                    while True:
                        if op_id != "MIGRATING" and self.nbi_k8s_connector.getOperationState(op_id) != "PROCESSING":
                            self.log[cName] += str(time.time()) + ","
                            timestamps = [float(ts) for ts in self.log[cName].split(',') if ts]
                            differences = [0]
                            for i in range(1, len(timestamps)):
                                differences.append((timestamps[i] - timestamps[i - 1])* 1000)
                            differences_str = ','.join(f"{diff:.6f}" for diff in differences)
                            #print(differences_str)
                            with open("results.csv", "a") as log_file:
                                log_file.write(differences_str + "\n")
                            self.log.pop(cName)
                            break

                    old_pod_status = new_pod_status