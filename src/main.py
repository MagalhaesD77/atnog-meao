import os
from nbi_k8s_connector import NBIConnector
from meao import MEAO
from flask import Flask, jsonify

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
        os.environ.get("OSS_HOSTNAME"),
        os.environ.get("KUBECTL_COMMAND"),
        os.environ.get("KUBECTL_CONFIG_PATH")
    )

    meao = MEAO(
        nbi_k8s_connector,
        int(os.environ.get("UPDATE_CONTAINER_IDS_FREQ")),
        os.environ.get("METRICS_COLLECTOR_KAFKA_TOPIC"),
        os.environ.get("UE_LATENCY_KAFKA_TOPIC"),
        {
            'bootstrap.servers': os.environ.get("KAFKA_SERVER"),
            'group.id': 'monitoring',
            'auto.offset.reset': 'latest'
        },
    )

    meao.start()

    app.run(host="0.0.0.0", port=8000)

if __name__ == "__main__":
    main()