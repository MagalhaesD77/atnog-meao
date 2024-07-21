import json
import subprocess
import yaml
import requests
import warnings
import time
from osmclient import client
from osmclient.common.exceptions import ClientException, OsmHttpException

class NBIConnector:

    def __init__(self, osm_hostname, oss_hostname, kubectl_command, kubectl_config_path) -> None:
        self.osm_hostname = osm_hostname
        self.kubectl_command = kubectl_command
        self.kubectl_config_path = kubectl_config_path
        self.nbi_client = client.Client(host=self.osm_hostname, port=9999,sol005=True)
        self.oss_hostname = oss_hostname
        kubectl_config = None
        while not kubectl_config:
            kubectl_config = self.getKubeConfig()
        with open(self.kubectl_config_path, 'w') as file:
            yaml.dump(kubectl_config["credentials"], file)
    
    def getKubeConfig(self):
        kubectl_config = None
        try:
            kubectl_config = self.callNBI(self.nbi_client.k8scluster.list)[0]
        except Exception as e:
            print("ERROR: Could not get kube config: {}".format(e))
            time.sleep(5)

        return kubectl_config
    
    def get_pod_status(self, namespace):
        command = (
            "{} --kubeconfig={} get pods -n {} -o=json".format(
                self.kubectl_command,
                self.kubectl_config_path,
                namespace,
            )
        )
        try:
            # Execute the kubectl command and capture the output
            pods = json.loads(subprocess.check_output(command.split()))['items']
        except subprocess.CalledProcessError as e:
            # Handle any errors if the command fails
            print("Error executing kubectl command:", e)
        pod_status = {pod['metadata']['name']: pod['status']['phase'] for pod in pods}
        return pod_status
    
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
            return nodeSpecs
        
        for node in node_info["items"]:
            nodeSpecs[node["metadata"]["labels"]["kubernetes.io/hostname"]] = {
                "num_cpu_cores": int(node["status"]["allocatable"]["cpu"]),
                "memory_size": int(node["status"]["allocatable"]["memory"][:-2])/pow(1024,2),
            }

        command = (
            "{} --kubeconfig={} -n cadvisor get pods -o=json".format(
                self.kubectl_command,
                self.kubectl_config_path,
            )
        )
        try:
            # Execute the kubectl command and capture the output
            cadvisor_pods = json.loads(subprocess.check_output(command.split()))
        except subprocess.CalledProcessError as e:
            # Handle any errors if the command fails
            print("Error executing kubectl command:", e)
            return nodeSpecs

        for cadvisor_pod in cadvisor_pods["items"]:
            if "nodeName" in cadvisor_pod["spec"]:
                nodeSpecs[cadvisor_pod["spec"]["nodeName"]]["cadvisor"] = cadvisor_pod["metadata"]["name"]

        return nodeSpecs
    
    def processMigrationPolicy(self, migration_policy, nodeSpecs, nodeName):
        if not migration_policy["enabled"]:
            return {
                "cpu_load_thresh": None,
                "mem_load_thresh": None,
                "mobility-migration-factor": None,
            }
        
        cpu_load_thresh = None
        mem_load_thresh = None
        mobility_migration_factor = None
        if "cpu-criteria" in migration_policy and nodeName in nodeSpecs:
            cpu_load_thresh = (migration_policy["cpu-criteria"]["allocated-cpu"]/nodeSpecs[nodeName]["num_cpu_cores"])*100
            cpu_surge_capacity = (migration_policy["cpu-criteria"]["cpu-surge-capacity"]/nodeSpecs[nodeName]["num_cpu_cores"])*100
        
        if "mem-criteria" in migration_policy and nodeName in nodeSpecs:
            mem_load_thresh = ((migration_policy["mem-criteria"]["allocated-mem"]/1024)/nodeSpecs[nodeName]["memory_size"])*100
            mem_surge_capacity = ((migration_policy["mem-criteria"]["mem-surge-capacity"]/1024)/nodeSpecs[nodeName]["memory_size"])*100

        if "mobility-criteria" in migration_policy:
            mobility_migration_factor = migration_policy["mobility-criteria"]["mobility-migration-factor"]

        return {
            "cpu_load_thresh": cpu_load_thresh,
            "cpu_surge_capacity": cpu_surge_capacity,
            "mem_load_thresh": mem_load_thresh,
            "mem_surge_capacity": mem_surge_capacity,
            "mobility-migration-factor": mobility_migration_factor,
        }


    def getContainerInfo(self, nodeSpecs):
        self.callNBI(self.nbi_client.__init__, host=self.osm_hostname, port=9999,sol005=True)
        ns_instances = self.callNBI(self.nbi_client.ns.list)
        mec_apps = self.callOSS("/mec-appis")
        
        containerInfo = {}

        if ns_instances == None:
            print('ERROR: Error calling OSM ns_instances endpoint')
            return containerInfo
        elif len(ns_instances) < 1:
            print('INFO: No deployed ns instances')
            return containerInfo
        elif 'code' in ns_instances[0].keys():
            print('ERROR: Error calling OSM ns_instances endpoint')
            return containerInfo
        
        if "error" in mec_apps:
            print('ERROR: Error calling OSS mec-appis endpoint')
            return containerInfo
        else:
            mec_apps = json.loads(mec_apps)

        for ns_instance in ns_instances:
            if "deployed" not in ns_instance["_admin"] or "K8s" not in ns_instance["_admin"]["deployed"]:
                continue
            ns_id = ns_instance["_id"]
            vnf_ids = ns_instance["constituent-vnfr-ref"]
            vnf_instances = {}
            for vnf_id in vnf_ids:
                vnfContent = self.callNBI(self.nbi_client.vnf.get, vnf_id)
                if vnfContent:
                    vnf_instances[vnfContent["member-vnf-index-ref"]] = vnfContent["_id"]

            kdu_instances = ns_instance["_admin"]["deployed"]["K8s"]
            for kdu in kdu_instances:
                kdu_instance = kdu["kdu-instance"]
                member_vnf_index = kdu["member-vnf-index"]
                namespace = kdu["namespace"]
                vnf_id = vnf_instances[member_vnf_index]

                command = (
                    "{} --kubeconfig={} --namespace={} get pods -l osm.etsi.org/ns-id={} -o=json".format(
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
                    return containerInfo
                
                for pod in k8s_info["items"]:
                    if "deletionGracePeriodSeconds" in pod["metadata"] and "deletionTimestamp" in pod["metadata"]:
                        continue
                    if "nodeName" in pod["spec"]:
                        nodeName = pod["spec"]["nodeName"]
                        migration_policy = None
                        for mec_app in mec_apps:
                            if mec_app["appi_id"] == ns_id and mec_app["vnf_id"] == vnf_id:
                                migration_policy = self.processMigrationPolicy(mec_app["migration_policy"], nodeSpecs, nodeName)
                                break
                        if "containerStatuses" in pod["status"]:
                            containers = pod["status"]["containerStatuses"]
                            for container in containers:
                                if "containerID" in container:
                                    id = container["containerID"]
                                    containerInfo[id.strip('"').split('/')[-1]] = {
                                        "ns_id": ns_id,
                                        "vnf_id": vnf_id,
                                        "kdu_id": kdu_instance,
                                        "node": nodeName,
                                        "migration_policy": migration_policy,
                                    }

        return containerInfo
    
    def migrate(self, cName, container, node):
        print("MIGRATING CONTAINER TO NODE {}".format(node))
        print("CONTAINER ID: {}".format(cName))
        print("NETWORK SERVICE ID: {}".format(container["ns_id"]))
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
        except (ConnectionError, ClientException, requests.exceptions.ReadTimeout) as e:
            print(f"An error occurred: {e}")
            return None
        
    def callOSS(self, endpoint):
        endpoint = self.oss_hostname + endpoint
        result = {'error': True, 'data': ''}

        try:
            with warnings.catch_warnings():
                warnings.filterwarnings("ignore", category=requests.packages.urllib3.exceptions.InsecureRequestWarning)
                r = requests.get(endpoint)
        except Exception as e:
            result['data'] = str(e)
            return result

        if r.status_code == requests.codes.ok:
            result['error'] = False

        result['data'] = r.text
        info = r.text

        return info
        
    def getOperationState(self, op_id):
        try:
            return self.callNBI(self.nbi_client.ns.get_op, op_id)["operationState"]
        except Exception as e:
            print("Error finding nslcmop:", e)
            return "NOT FOUND"
        