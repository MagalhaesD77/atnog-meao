from ...threads.mec_apps_thread import containers
from ...threads.websocket_service_thread import lat_queue


def callback(data):
    if "containerInfo" in data and "nodeSpecs" in data:
        print(data)
        idsMonitored = []
        for containerName, container in data["containerInfo"].items():
            idsMonitored.append(containerName)
            node_specs = data["nodeSpecs"]
            if containerName not in containers:
                containers[containerName] = {
                    "ns": container["ns_id"],
                    "node": container["node"],
                    "node_specs": node_specs[container["node"]],
                }
                containers[containerName]["node_specs"]["prev_cpu"] = 0
                containers[containerName]["node_specs"]["prev_timestamp"] = 0
        idsToDelete = []
        for container_id in containers.keys():
            if container_id not in idsMonitored:
                idsToDelete.append(container_id)
        for id in idsToDelete:
            del containers[id]
    elif "warning" in data:
        pass