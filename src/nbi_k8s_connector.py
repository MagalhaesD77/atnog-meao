import json
import subprocess
import yaml
import time
from osmclient import client
from osmclient.common.exceptions import ClientException, OsmHttpException

class NBIConnector:

    def __init__(self, osm_hostname, kubectl_command, kubectl_config_path) -> None:
        self.osm_hostname = osm_hostname
        self.kubectl_command = kubectl_command
        self.kubectl_config_path = kubectl_config_path
        self.nbi_client = client.Client(host=self.osm_hostname, port=9999,sol005=True)
        try:
            kubectl_config = self.callNBI(self.nbi_client.k8scluster.list)[0]
        except Exception as e:
            print("ERROR: Could not get kube config: {}".format(e))
            exit(1)
        with open(self.kubectl_config_path, 'w') as file:
            yaml.dump(kubectl_config["credentials"], file)
    
    def getNodeSpecs(self):
        nodeSpecs = {}

        command = (
            "{} --kubeconfig={} get nodes -o=json".format(
                self.kubectl_command,
                self.kubectl_config_path,
            )
        )
        try:
            # Execute the kubectl command and capture the output
            node_info = json.loads(subprocess.check_output(command.split()))
        except subprocess.CalledProcessError as e:
            # Handle any errors if the command fails
            print("Error executing kubectl command:", e)
            return None

        for node in node_info["items"]:
            nodeSpecs[node["metadata"]["labels"]["kubernetes.io/hostname"]] = {
                "num_cpu_cores": int(node["status"]["allocatable"]["cpu"]),
                "memory_size": int(node["status"]["allocatable"]["memory"][:-2])/pow(1024,2),
            }

        return nodeSpecs

    def getContainerInfo(self, nodeSpecs):
        self.callNBI(self.nbi_client.__init__, host=self.osm_hostname, port=9999,sol005=True)
        ns_instances = self.callNBI(self.nbi_client.ns.list)
        
        if len(ns_instances) < 1:
            print('ERROR: No deployed ns instances')
        elif 'code' in ns_instances[0].keys():
            print('ERROR: Error calling ns_instances endpoint')

        containerInfo = []

        for ns_instance in ns_instances:
            if "deployed" not in ns_instance["_admin"] or "K8s" not in ns_instance["_admin"]["deployed"]:
                continue
            ns_id = ns_instance["_id"]
            vnf_ids = ns_instance["constituent-vnfr-ref"]
            vnf_instances = {}
            for vnf_id in vnf_ids:
                vnfContent = self.callNBI(self.nbi_client.vnf.get, vnf_id)
                vnfdsContent = self.callNBI(self.nbi_client.vnfd.get, vnfContent["vnfd-id"])
                vnf_instances[vnfContent["member-vnf-index-ref"]] = {
                    "vnf_id": vnfContent["_id"],
                    "cpu_req": int(vnfdsContent["virtual-compute-desc"][0]["virtual-cpu"]["num-virtual-cpu"]),
                    "mem_req": int(vnfdsContent["virtual-compute-desc"][0]["virtual-memory"]["size"])/1024,
                }

            kdu_instances = ns_instance["_admin"]["deployed"]["K8s"]
            for kdu in kdu_instances:
                kdu_instance = kdu["kdu-instance"]
                member_vnf_index = kdu["member-vnf-index"]
                namespace = kdu["namespace"]
                vnf_id = vnf_instances[member_vnf_index]["vnf_id"]
                cpu_req = vnf_instances[member_vnf_index]["cpu_req"]
                mem_req = vnf_instances[member_vnf_index]["mem_req"]

                command = (
                    "{} --kubeconfig={} --namespace={} get pods -l ns_id={} -o=json".format(
                        self.kubectl_command,
                        self.kubectl_config_path,
                        namespace,
                        ns_id,
                    )
                )
                try:
                    # Execute the kubectl command and capture the output
                    k8s_info = json.loads(subprocess.check_output(command.split()))
                except subprocess.CalledProcessError as e:
                    # Handle any errors if the command fails
                    print("Error executing kubectl command:", e)
                    return None
                
                for pod in k8s_info["items"]:
                    if "deletionGracePeriodSeconds" in pod["metadata"] and "deletionTimestamp" in pod["metadata"]:
                        continue
                    if "nodeName" in pod["spec"]:
                        nodeName = pod["spec"]["nodeName"]
                        cpu_load_thresh = (cpu_req/nodeSpecs[nodeName]["num_cpu_cores"])*100
                        mem_load_thresh = (mem_req/nodeSpecs[nodeName]["memory_size"])*100
                        containers = pod["status"]["containerStatuses"]
                        for container in containers:
                            if "containerID" in container:
                                id = container["containerID"]
                                containerInfo.append({
                                    "id": id.strip('"').split('/')[-1],
                                    "ns_id": ns_id,
                                    "vnf_id": vnf_id,
                                    "kdu_id": kdu_instance,
                                    "node": nodeName,
                                    "cpu_load_thresh": cpu_load_thresh,
                                    "mem_load_thresh": mem_load_thresh,
                                })

        return containerInfo
    
    def migrate(self, container, node):
        print("MIGRATING CONTAINER {} TO NODE {}".format(container, node))
        try:
            return self.callNBI(
                self.nbi_client.ns.migrate_k8s,
                container["ns_id"],
                migrate_dict = {
                    "vnfInstanceId": container["vnf_id"],
                    "migrateToHost": node,
                    "kdu": {
                        "kduId": container["kdu_id"],
                        "kduCountIndex": 0,
                    }
                })
        except Exception as e:
            print("ERROR: {}".format(e))

    def callNBI(self, func, *args, **kwargs):
        try:
            return func(*args, **kwargs)
        except (OsmHttpException) as e:
            self.nbi_client = client.Client(host=self.osm_hostname, port=9999,sol005=True)
            print(f"An error occurred: {e}")
            return func(*args, **kwargs)
        except (ConnectionError, ClientException) as e:
            print(f"An error occurred: {e}")
            return None
        
    def getOperationState(self, op_id):
        return self.callNBI(self.nbi_client.ns.get_op, op_id)["operationState"]
        