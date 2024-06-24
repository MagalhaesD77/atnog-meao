import json
import asyncio
import time
import threading
from confluent_kafka import Consumer, KafkaError
import random

class MEAO:
    def __init__(self, nbi_k8s_connector, update_container_ids_freq, kafka_topic, kafka_consumer_conf) -> None:
        self.nbi_k8s_connector = nbi_k8s_connector
        self.update_container_ids_freq = update_container_ids_freq
        self.kafka_topic = kafka_topic
        self.kafka_consumer_conf = kafka_consumer_conf
        self.migratingContainers = {}
        self.nodeSpecs = self.nbi_k8s_connector.getNodeSpecs()
        print("Node Specs: " + str(self.nodeSpecs))
        self.containerInfo = self.nbi_k8s_connector.getContainerInfo(self.nodeSpecs)
        print("Container Info: " + str(self.containerInfo))
        self.cpu_history = {}


    def start(self):
        # Create threads
        consume_thread = threading.Thread(target=self.consume_messages)
        update_thread = threading.Thread(target=self.update_container_ids)

        # Start threads
        consume_thread.start()
        update_thread.start()

    def migrationAlgorithm(self, container, cpuLoad, memLoad):
        if (
            (container["migration_policy"]["cpu_load_thresh"] and cpuLoad > container["migration_policy"]["cpu_load_thresh"]) 
            or (container["migration_policy"]["mem_load_thresh"] and memLoad > container["migration_policy"]["mem_load_thresh"])
        ):
            return True
        return False

    def calcMetrics(self, cName, container, values):
        #print(json.dumps(values, indent=2))

        print("-------------------------------------------------------")
        print("Container ID:", container["id"])
        print("Timestamp:", values["timestamp"])


        # Memory
        memUsage = values["container_stats"]["memory"]["usage"]
        #print("Memory Usage:", memUsage)
        memLoad = (memUsage/(self.nodeSpecs[container["node"]]["memory_size"]*pow(1024,3))) * 100
        print("Memory Load:", memLoad)


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
                cpuLoad = ((cpuDelta / systemDelta) / self.nodeSpecs[container["node"]]["num_cpu_cores"]) * 100
                print("CPU Load:", cpuLoad)
                if cpuLoad > 100:
                    print(self.cpu_history[cName]["previousCPU"])
                    print(currentCPU)
                    print(cpuDelta)
                    print(self.cpu_history[cName]["previousSystem"])
                    print(timestamp)
                    print(systemDelta)
                    print(cpuLoad)
                    cpuLoad = 0
        self.cpu_history[cName]["previousCPU"] = currentCPU
        self.cpu_history[cName]["previousSystem"] = timestamp

        #print("Network RX Bytes:", values["container_stats"]["network"]["rx_bytes"])
        #print("Network TX Bytes:", values["container_stats"]["network"]["tx_bytes"])
        #if values["container_stats"]["diskio"] != {}:
            #print("Disk IO Read:", values["container_stats"]["diskio"]["io_service_bytes"][0]["stats"]["Read"])
            #print("Disk IO Write:", values["container_stats"]["diskio"]["io_service_bytes"][0]["stats"]["Write"])
        print("-------------------------------------------------------")

        metrics = {
            "cpuLoad": cpuLoad,
            "memLoad": memLoad,
        }

        return metrics
    
    async def processContainer(self, cName, container, values):
        metrics = self.calcMetrics(cName, container, values)

        res = self.migrationAlgorithm(container, metrics["cpuLoad"], metrics["memLoad"])
        if res and container["id"] not in self.migratingContainers:
            op_id = self.nbi_k8s_connector.migrate(container, random.choice(list(self.nodeSpecs.keys())))
            self.migratingContainers[container["id"]] = op_id

    def consume_messages(self):
        # Create Kafka consumer
        consumer = Consumer(self.kafka_consumer_conf)

        # Subscribe to the topic
        consumer.subscribe([self.kafka_topic])

        try:
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
                for container in self.containerInfo:
                    if container["id"] in cName:
                        asyncio.run(self.processContainer(cName, container, values))

        except KeyboardInterrupt:
            # Stop consumer on keyboard interrupt
            consumer.close()

    def update_container_ids(self):
        while True:
            time.sleep(self.update_container_ids_freq)
            self.containerInfo = self.nbi_k8s_connector.getContainerInfo(self.nodeSpecs)
            idsToDelete = []
            for container_id, op_id in self.migratingContainers.items():
                if self.nbi_k8s_connector.getOperationState(op_id) != "PROCESSING":
                    idsToDelete.append(container_id)
            for id in idsToDelete:
                self.migratingContainers.pop(id)
            print("Container Info: " + str(self.containerInfo))
