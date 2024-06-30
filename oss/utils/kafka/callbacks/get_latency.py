from ...threads.container_info_thread import containers
from ...threads.websocket_service_thread import lat_queue


def callback(data):
    for container_id in containers.keys():
        data["node"] = containers[container_id]["node"]
        data["appi_id"] = containers[container_id]["ns"]
        break
    lat_queue.put(data)