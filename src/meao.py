import json
import asyncio
import time
import threading
from confluent_kafka import Consumer, KafkaError
import random

class MEAO:
    def __init__(self, nbi_k8s_connector, update_container_ids_freq, metrics_collector_kafka_topic, ue_latency_kafka_topic, kafka_consumer_conf) -> None:
        self.nbi_k8s_connector = nbi_k8s_connector
        self.update_container_ids_freq = update_container_ids_freq
        self.metrics_collector_kafka_topic = metrics_collector_kafka_topic
        self.ue_latency_kafka_topic = ue_latency_kafka_topic
        self.kafka_consumer_conf = kafka_consumer_conf
        self.migratingContainers = {}
        self.nodeSpecs = self.nbi_k8s_connector.getNodeSpecs()
        print("Node Specs: " + str(self.nodeSpecs))
        self.containerInfo = self.nbi_k8s_connector.getContainerInfo(self.nodeSpecs)
        print("Container Info: " + str(self.containerInfo))
        self.cpu_history = {}


    def start(self):
        # Create threads
        read_metrics_collector = threading.Thread(target=self.read_metrics_collector)
        read_ue_latency = threading.Thread(target=self.read_ue_latency)
        update_thread = threading.Thread(target=self.update_container_ids)

        # Start threads
        read_metrics_collector.start()
        read_ue_latency.start()
        update_thread.start()

    def get_node_specs(self, hostname=None):
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

    def migrationAlgorithm(self, cName):
        container = self.containerInfo[cName]
        currentNode = container["node"]
        if (
            not container["migration_policy"] 
            or cName in self.migratingContainers
            or "cpuLoad" not in self.nodeSpecs[currentNode]
            or "memLoad" not in self.nodeSpecs[currentNode]
        ):
            return None
        
        resourceTargetNode = None
        latencyTargetNode = None

        if (
            (container["migration_policy"]["cpu_load_thresh"] and "cpuLoad" in container)
            or (container["migration_policy"]["mem_load_thresh"] and "memLoad" in container)
        ):
            resourceTargetNode = self.resourceMigrationAlgorithm(container)
        if container["migration_policy"]["mobility-migration-factor"] and "ue-lats" in container:
            latencyTargetNode = self.latencyMigrationAlgorithm(container)

        finalTargetNode = None
        if (
            (resourceTargetNode and resourceTargetNode in self.nodeSpecs.keys())
            and (latencyTargetNode and latencyTargetNode in self.nodeSpecs.keys())
        ):
            if resourceTargetNode == latencyTargetNode:
                finalTargetNode = resourceTargetNode
        elif resourceTargetNode and resourceTargetNode in self.nodeSpecs.keys():
            finalTargetNode = resourceTargetNode
        elif latencyTargetNode and latencyTargetNode in self.nodeSpecs.keys():
            finalTargetNode = latencyTargetNode

        if finalTargetNode:
            op_id = self.nbi_k8s_connector.migrate(cName, container, finalTargetNode)
            self.migratingContainers[cName] = op_id

    def min_usage_node(self):
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

    def resourceMigrationAlgorithm(self, container):
        cpuLoad = container["cpuLoad"]
        memLoad = container["memLoad"]
        
        # TEM DE SE MUDAR ESTA CONDIÇÃO
        if (
            (cpuLoad > container["migration_policy"]["cpu_load_thresh"]) 
            or (memLoad > container["migration_policy"]["mem_load_thresh"])
        ):
            usage = self.nodeSpecs[container["node"]]["cpuLoad"] + self.nodeSpecs[container["node"]]["memLoad"]
            min_usage_node, min_usage = self.min_usage_node()
            if min_usage_node != container["node"] and min_usage < usage:
                return min_usage_node
        
        return None
    
    def min_lat_MEH(self, ue_lats):
        min_lat_meh = None
        min_lat = 0
        for meh, meh_lat in ue_lats.items():
            if not min_lat_meh or meh_lat < min_lat:
                min_lat_meh = meh
                min_lat = meh_lat
        return min_lat_meh, min_lat
        
    def latencyMigrationAlgorithm(self, container):
        ue_lats = container["ue-lats"]

        current_meh = container["node"]
        current_meh_lat = ue_lats[current_meh]

        min_lat_meh, min_lat = self.min_lat_MEH(ue_lats)

        if min_lat_meh != current_meh and min_lat < container["migration_policy"]["mobility-migration-factor"]*current_meh_lat:
            return min_lat_meh
        
        return None
    
    async def processContainerMetrics(self, cName, values, container=None):
        metrics = self.calcMetrics(cName, values, container)

        if not container:
            self.nodeSpecs[cName]["cpuLoad"] = metrics["cpuLoad"]
            self.nodeSpecs[cName]["memLoad"] = metrics["memLoad"]
        else:        
            self.containerInfo[cName]["cpuLoad"] = metrics["cpuLoad"]
            self.containerInfo[cName]["memLoad"] = metrics["memLoad"]
            self.migrationAlgorithm(cName)
    
    async def processContainerLatencies(self, cName, values):
        self.containerInfo[cName]["ue-lats"] = values
        self.migrationAlgorithm(cName)

    def calcMetrics(self, cName, values, container=None, silent=True):
        if container:
            memory_size = (self.nodeSpecs[container["node"]]["memory_size"]*pow(1024,3))
            num_cpu_cores = self.nodeSpecs[container["node"]]["num_cpu_cores"]
        else:
            memory_size = (self.nodeSpecs[cName]["memory_size"]*pow(1024,3))
            num_cpu_cores = self.nodeSpecs[cName]["num_cpu_cores"]

        # CPU
        timestampParts = values["timestamp"].split(':')
        timestamp = (float(timestampParts[-3][-2:])*pow(60,2) + float(timestampParts[-2])*60 + float(timestampParts[-1][:-1])) * pow(10, 9)
        currentCPU = values["container_stats"]["cpu"]["usage"]["total"]
        cpuLoad = 0
        if cName not in self.cpu_history.keys():
            self.cpu_history[cName] = {}
            self.cpu_history[cName]["previousCPU"] = 0
            self.cpu_history[cName]["previousSystem"] = 0
        if self.cpu_history[cName]["previousCPU"] != 0 and self.cpu_history[cName]["previousSystem"] != 0:
            # Calculate CPU usage delta
            cpuDelta = currentCPU - self.cpu_history[cName]["previousCPU"]
            #print("CPU Delta: ", cpuDelta)

            # Calculate System delta
            systemDelta = timestamp - self.cpu_history[cName]["previousSystem"]
            #print("System Delta", systemDelta)

            if systemDelta > 0.0 and cpuDelta >= 0.0:
                cpuLoad = ((cpuDelta / systemDelta) / num_cpu_cores) * 100
                if cpuLoad > 100:
                    cpuLoad = 0
        self.cpu_history[cName]["previousCPU"] = currentCPU
        self.cpu_history[cName]["previousSystem"] = timestamp

        # Memory
        memUsage = values["container_stats"]["memory"]["usage"]
        #print("Memory Usage:", memUsage)
        memLoad = (memUsage/memory_size) * 100

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

        metrics = {
            "cpuLoad": cpuLoad,
            "memLoad": memLoad,
        }

        return metrics

    def updateDict(self, oldDict, updatedDict):
        keys_to_keep_nodeSpecs = set(oldDict.keys()).intersection(updatedDict.keys())
        keys_to_remove_nodeSpecs = set(oldDict.keys()) - keys_to_keep_nodeSpecs
        for key in keys_to_remove_nodeSpecs:
            oldDict.pop(key)
        
        keys_to_add_nodeSpecs = set(updatedDict.keys()) - set(oldDict.keys())
        for key in keys_to_add_nodeSpecs:
            oldDict[key] = updatedDict[key]

        return oldDict

    def read_metrics_collector(self):
        # Create Kafka consumer
        consumer = Consumer(self.kafka_consumer_conf)

        # Subscribe to the topic
        consumer.subscribe([self.metrics_collector_kafka_topic])

        try:
            print("Listening to Kafka on topic {}....".format(self.metrics_collector_kafka_topic))
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
                if cName == "/":
                    machine_name = values["machine_name"]
                    if not any(machine_name in nodeInfo.values() for node, nodeInfo in self.nodeSpecs.items()):
                        continue
                    for node, nodeInfo in self.nodeSpecs.items():
                        if nodeInfo["cadvisor"] == machine_name:
                            asyncio.run(self.processContainerMetrics(node, values))
                            break
                for containerName, container in self.containerInfo.items():
                    if containerName in cName:
                        asyncio.run(self.processContainerMetrics(containerName, values, container))

        except KeyboardInterrupt:
            # Stop consumer on keyboard interrupt
            consumer.close()

    def read_ue_latency(self):
        # Create Kafka consumer
        consumer = Consumer(self.kafka_consumer_conf)

        # Subscribe to the topic
        consumer.subscribe([self.ue_latency_kafka_topic])

        try:
            print("Listening to Kafka on topic {}....".format(self.ue_latency_kafka_topic))
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
                for containerName in self.containerInfo.keys():
                    asyncio.run(self.processContainerLatencies(containerName, values))

        except KeyboardInterrupt:
            # Stop consumer on keyboard interrupt
            consumer.close()

    def update_container_ids(self):
        while True:
            time.sleep(self.update_container_ids_freq)

            # Update node specs
            updatedNodeSpecs = self.nbi_k8s_connector.getNodeSpecs()
            if not updatedNodeSpecs:
                self.nodeSpecs = None
            else:
                self.nodeSpecs = self.updateDict(self.nodeSpecs, updatedNodeSpecs)

            # Update container info
            updatedContainerInfo = self.nbi_k8s_connector.getContainerInfo(self.nodeSpecs)
            if not updatedContainerInfo:
                self.containerInfo = None
            else:
                self.containerInfo = self.updateDict(self.containerInfo, updatedContainerInfo)
            
            # Clean up migrating containers
            idsToDelete = [
                container_id 
                for container_id, op_id in self.migratingContainers.items() 
                if self.nbi_k8s_connector.getOperationState(op_id) != "PROCESSING"
            ]
            
            for container_id in idsToDelete:
                self.migratingContainers.pop(container_id)
            print("Container Info: " + json.dumps(self.containerInfo, indent=2))
            print("Node Specs: " + json.dumps(self.nodeSpecs, indent=2))