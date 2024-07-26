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
            response = requests.get("http://meao-monitoring:8000/containerInfo")
            print(response)
            idsMonitored = []
            for containerName, container in (response.json()["ContainerInfo"]).items():
                idsMonitored.append(containerName)
                node_specs = requests.get(
                    "http://meao-monitoring:8000/nodeSpecs/" + container["node"]
                ).json()
                if containerName not in containers:
                    containers[containerName] = {
                        "ns": container["ns_id"],
                        "node": container["node"],
                        "node_specs": node_specs["NodeSpecs"],
                    }
                    containers[containerName]["node_specs"]["prev_cpu"] = 0
                    containers[containerName]["node_specs"]["prev_timestamp"] = 0
            idsToDelete = []
            for container_id in containers.keys():
                if container_id not in idsMonitored:
                    idsToDelete.append(container_id)
            for id in idsToDelete:
                del containers[id]

        except Exception as e:
            pass

        time.sleep(15)
