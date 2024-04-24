# OSM NBI configs
NBI_URL="https://10.255.32.88:9999/osm"
USERNAME= PASSWORD = PROJECT = "admin"

# Specs
RAM = 8
NUM_CORES = 8

# Thresholds
CPU_LOAD_THRESH = 80
MEM_LOAD_THRESH = 60

# Kafka broker configuration
bootstrap_servers = '10.255.32.88:10567'

# Kafka topic to consume from
topic = 'k8s-cluster'
containerInfo = []

# Kafka consumer configuration
consumer_conf = {
    'bootstrap.servers': bootstrap_servers,
    'group.id': 'monitoring',
    'auto.offset.reset': 'latest'
}

kubectl_command = "kubectl"
kube_config_path = ".kube/config"