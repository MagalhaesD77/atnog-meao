import threading
import time

import requests
from cherrypy.process import plugins

containers = {}


class ContainerInfoThread(plugins.SimplePlugin):
    """Background thread that mapps container id to ns id"""

    def __init__(self, bus):
        super().__init__(bus)
        self.t = None

    def start(self):
        """Plugin entrypoint"""

        self.t = threading.Thread(target=get_containers_info)
        self.t.daemon = True
        self.t.start()


def get_containers_info():
    while True:
        try:
            response = requests.get("http://meao-migration:8000/containerInfo")
            idsMonitored = []
            for container in response.json()["ContainerInfo"]:
                idsMonitored.append(container["id"])
                node_specs = requests.get(
                    "http://meao-migration:8000/nodeSpecs/" + container["node"]
                ).json()
                if container["id"] not in containers:
                    containers[container["id"]] = {
                        "ns": container["ns_id"],
                        "node": container["node"],
                        "node_specs": node_specs["NodeSpecs"],
                    }
                    containers[container["id"]]["node_specs"]["prev_cpu"] = 0
                    containers[container["id"]]["node_specs"]["prev_timestamp"] = 0
            idsToDelete = []
            for container_id in containers.keys():
                if container_id not in idsMonitored:
                    idsToDelete.append(container_id)
            for id in idsToDelete:
                del containers[id]
            print(containers)

        except Exception as e:
            pass

        time.sleep(15)
