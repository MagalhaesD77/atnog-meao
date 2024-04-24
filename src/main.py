from config import *
from nbi_connector import NBIConnector
from meao import MEAO

def main():
    nbi_connector = NBIConnector(
        NBI_URL,
        KUBECTL_COMMAND,
        KUBECTL_CONFIG_PATH
    )

    meao = MEAO(
        nbi_connector,
        KAFKA_TOPIC,
        KAFKA_CONSUMER_CONF,
        NUM_CPU_CORES,
        RAM_SIZE,
        CPU_LOAD_THRESH,
        MEM_LOAD_THRESH,
    )

    meao.start()

if __name__ == "__main__":
    main()