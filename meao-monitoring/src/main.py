import os
from nbi_k8s_connector import NBIConnector
from meao import MEAO
from flask import Flask, jsonify
import json

app = Flask(__name__)
meao = None

@app.route("/containerInfo", methods=["GET"])
def get_container_info():
    global meao
    return jsonify(ContainerInfo=meao.get_container_ids())


@app.route("/nodeSpecs", methods=["GET"])
def get_node_specs():
    global meao
    return jsonify(NodeSpecs=meao.get_node_specs())


@app.route("/nodeSpecs/<hostname>", methods=["GET"])
def get_node_specs_hostname(hostname):
    global meao
    return jsonify(NodeSpecs=meao.get_node_specs(hostname))


@app.route("/nodeSpecs/update", methods=["GET"])
def update_node_specs():
    global meao
    meao.update_node_specs()
    return jsonify(NodeSpecs=meao.get_node_specs())

def main():
    global meao
    nbi_k8s_connector = NBIConnector(
        os.environ.get("OSM_HOSTNAME"),
        os.environ.get("KUBECTL_COMMAND"),
        os.environ.get("KUBECTL_CONFIG_PATH")
    )

    kafka_producer_config = json.loads(os.environ.get("KAFKA_PRODUCER_CONFIG", '{"bootstrap.servers": "localhost:9092"}'))
    kafka_consumer_config = json.loads(os.environ.get("KAFKA_CONSUMER_CONFIG", '{"bootstrap.servers": "localhost:9092", "group.id": "monitoring", "auto.offset.reset": "latest"}'))
    meao = MEAO(
        nbi_k8s_connector,
        os.environ.get("METRICS_COLLECTOR_KAFKA_TOPIC"),
        os.environ.get("UE_LATENCY_KAFKA_TOPIC"),
        os.environ.get("MEAO_OSS_KAFKA_TOPIC"),
        int(os.environ.get("SEND_CONTAINER_INFO_FREQ")),
        kafka_consumer_config,
        kafka_producer_config
    )

    meao.start()

    app.run(host="0.0.0.0", port=8000)

if __name__ == "__main__":
    main()