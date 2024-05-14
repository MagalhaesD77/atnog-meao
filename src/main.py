import os

from nbi_k8s_connector import NBIConnector
from meao import MEAO

def main():
    nbi_k8s_connector = NBIConnector(
        os.environ.get("NBI_URL"),
        os.environ.get("KUBECTL_COMMAND"),
        os.environ.get("KUBECTL_CONFIG_PATH")
    )

    meao = MEAO(
        nbi_k8s_connector,
        int(os.environ.get("UPDATE_CONTAINER_IDS_FREQ")),
        os.environ.get("KAFKA_TOPIC"),
        {
            'bootstrap.servers': os.environ.get("KAFKA_SERVER"),
            'group.id': 'monitoring',
            'auto.offset.reset': 'latest'
        },
        int(os.environ.get("CPU_LOAD_THRESH")),
        int(os.environ.get("MEM_LOAD_THRESH")),
    )

    meao.start()

if __name__ == "__main__":
    main()