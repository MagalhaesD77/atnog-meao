import json
from time import sleep
from confluent_kafka import Consumer, KafkaError
import threading

class MEAO:
    def __init__(self, nbi_connector, kafka_topic, kafka_consumer_conf, num_cpu_cores, ram_size, cpu_load_thresh, mem_load_thresh) -> None:
        self.nbi_connector = nbi_connector
        self.kafka_topic = kafka_topic
        self.kafka_consumer_conf = kafka_consumer_conf
        self.num_cpu_cores = num_cpu_cores
        self.ram_size = ram_size
        self.cpu_load_thresh = cpu_load_thresh
        self.mem_load_thresh = mem_load_thresh

    def start(self):
        # Create threads
        consume_thread = threading.Thread(target=self.consume_messages)
        update_thread = threading.Thread(target=self.update_container_ids)

        # Start threads
        consume_thread.start()
        update_thread.start()

    def migrationAlgorithm(self, cpuLoad, memLoad):
        if cpuLoad > self.cpu_load_thresh or memLoad > self.mem_load_thresh:
            return True
        return False

    def calcMetrics(self, cName, values, previousCPU, previousSystem):
        #print(json.dumps(values, indent=2))

        print("-------------------------------------------------------")
        print("Container Name:", cName)
        print("Timestamp:", values["timestamp"])


        # Memory
        memUsage = values["container_stats"]["memory"]["usage"]
        #print("Memory Usage:", memUsage)
        memLoad = (memUsage/(self.ram_size*pow(1024,3))) * 100
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
                cpuLoad = ((cpuDelta / systemDelta) / self.num_cpu_cores) * 100
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

    def consume_messages(self):
        global containerInfo

        # Create Kafka consumer
        consumer = Consumer(self.kafka_consumer_conf)

        # Subscribe to the topic
        consumer.subscribe([self.kafka_topic])

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
                        metrics = self.calcMetrics(cName, values, metrics["previousCPU"], metrics["previousSystem"])

                        res = self.migrationAlgorithm(metrics["cpuLoad"], metrics["memLoad"])
                        self.nbi_connector.migrate(container, res)

        except KeyboardInterrupt:
            # Stop consumer on keyboard interrupt
            consumer.close()

    def update_container_ids(self):
        global containerInfo
        while True:
            containerInfo = self.nbi_connector.getContainerInfo()
            print("Container Info: " + str(containerInfo))
            sleep(5)  # Wait for 5 seconds before updating again
