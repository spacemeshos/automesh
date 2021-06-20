from datetime import datetime
import getpass
import os
import sys
import random
import string
import unittest
from unittest.suite import _DebugResult

from kubernetes import client
from kubernetes import config as k8s_config
import yaml

from automeshion import pod
from automeshion import config as conf
from automeshion.context import Context, ES
from automeshion.k8s_handler import (
    add_elastic_cluster, add_kibana_cluster, add_fluent_bit_cluster,
    wait_for_to_be_ready,
)
from automeshion.misc import CoreV1ApiClient
from automeshion.node_pool_deployer import NodePoolDep
from automeshion.setup_utils import setup_bootstrap_in_namespace, setup_clients_in_namespace
from automeshion.utils import (
    api_call, wait_for_elk_cluster_ready,  wait_genesis, get_genesis_time_delta,
)
from app_engine.gcloud_tasks.add_task_to_queue import create_google_cloud_task

class DeploymentInfo:
    def __init__(self, dep_id):
        self.deployment_name = ''
        self.deployment_id = dep_id
        self.pods = []

    def __str__(self):
        ret_str = f"DeploymentInfo:\n\tdeployment name: {self.deployment_name}\n\t"
        ret_str += f"deployment id: {self.deployment_id}\n\tpods number: {len(self.pods)}"
        return ret_str


class NetworkInfo:
    def __init__(self, namespace, bs_deployment_info, cl_deployment_info):
        self.namespace = namespace
        self.bootstrap = bs_deployment_info
        self.clients = cl_deployment_info


class NetworkDeploymentInfo:
    def __init__(self, dep_id, bs_deployment_info, cl_deployment_info):
        self.deployment_name = ''
        self.deployment_id = dep_id
        self.bootstrap = bs_deployment_info
        self.clients = cl_deployment_info

    def __str__(self):
        ret_str = f"NetworkDeploymentInfo:\n\tdeployment name: {self.deployment_name}\n\t"
        ret_str += f"deployment id: {self.deployment_id}\n\tbootstrap info:\n\t{self.bootstrap}\n\t"
        ret_str += f"client info:\n\t{self.clients}"
        return ret_str


NAMESPACE_SUFFIX_LEN = 8
def random_namespace(length=8):
    # Just alphanumeric characters
    chars = string.ascii_lowercase + string.digits
    suffix = ''.join((random.choice(chars)) for x in range(NAMESPACE_SUFFIX_LEN))
    return '-'.join((getpass.getuser(), suffix))


class TestCase(unittest.TestCase):
    """
    TestCase is used for spacemesh system test usually ran by the ci.
    TestCase brings up a k8s namespace based on a config file.
    each test method has access to the class methods:

    :param config_path: the path to the config file, relative paths start
    at the project's root
    :type string: boolean
    :param namespace: the class's name space
    :type namespace: string
    :param config: the config file
    :type config: a map
    :param network: the test network
    :type network: NetworkDeploymentInfo

    :Example:

    class MyTest(automeshion.TestCase):
        config_path = "my_test/config.yaml"

        def test_something(self):
            ....

    """

    def setup(self):
        self.namespace = random_namespace()
        self.index_date = datetime.utcnow().date().strftime("%Y.%m.%d")
        print(f"creating namespace {self.namespace}")
        with open(self.config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        """
        self.load_k8s_config()
        self.set_namespace()
        self.set_docker_images()
        self.add_elk()
        self.add_node_pool()
        pod.create_pod(conf.CURL_POD_FILE, self.namespace)
        self.bootstrap = self.setup_bootstrap()
        self.start_poet()
        self.clients = self.setup_clients()
        self.network = NetworkDeploymentInfo(dep_id=self.namespace,
            bs_deployment_info=self.bootstrap, cl_deployment_info=self.clients)
        # genesis time = when clients have been created + delta =
        # time.now - time it took pods to come up + delta
        wait_genesis(get_genesis_time_delta(
            self.config['genesis_delta']), self.config['genesis_delta'])
        """

    def set_docker_images(self):
        docker_image = os.getenv('CLIENT_DOCKER_IMAGE', '')
        if docker_image:
            print("++Set docker images to: {0}".format(docker_image))
            self.config['bootstrap']['image'] = docker_image
            self.config['client']['image'] = docker_image
            if 'clientv2' in self.config.keys():
                print(self.config['clientv2'])
                # some should not be replaced!
                if self.config['clientv2'].get('noreplace', False):
                    print("not replacing clientv2 docker image since replace is set to False")
                else:
                    print("Set docker clientv2 images to: {0}".format(docker_image))
                    self.config['clientv2']['image'] = docker_image
            else:
                print("no other config")
                print(self.config.keys())

    def load_k8s_config(self):
        kube_config_var = os.getenv('KUBECONFIG', '~/.kube/config')
        kube_config_path = os.path.expanduser(kube_config_var)
        print("kubeconfig file is: {0}".format(kube_config_path))
        if os.path.isfile(kube_config_path):
            kube_config_context = Context().get()
            print("Loading config: {0} context: {1}".format(
                kube_config_path, kube_config_context))
            k8s_config.load_kube_config(config_file=kube_config_path, context=kube_config_context)
        else:
            # Assuming in cluster config
            try:
                print("Loading incluster config")
                k8s_config.load_incluster_config()
            except Exception as e:
                raise Exception("KUBECONFIG file not found: {0}\nException: {1}".format(kube_config_path, e))

    def set_namespace(self):
        v1 = CoreV1ApiClient()
        if self.config['namespace'] == '':
            self.config['namespace'] = self.namespace

        print("\nRun tests in namespace: {0}".format(self.config['namespace']))
        namespaces_list = [ns.metadata.name for ns in v1.list_namespace().items]
        if self.config['namespace'] in namespaces_list:
            raise ValueError(f"namespace: {self.config['namespace']} already exists!")

        body = client.V1Namespace()
        body.metadata = client.V1ObjectMeta(name=self.config['namespace'])
        v1.create_namespace(body)

    def add_elk(self):
        # get today's date for filebeat data index
        add_elastic_cluster(self.namespace)
        add_fluent_bit_cluster(self.namespace)
        add_kibana_cluster(self.namespace)
        wait_for_elk_cluster_ready(self.namespace)


    def add_node_pool(self):
        """
        memory should be represented by number of megabytes, \d+M

        :return:
        """
        deployer = NodePoolDep(self.config)
        _, time_elapsed = deployer.add_node_pool()
        print(f"total time waiting for clients node pool creation: {time_elapsed}")
        # wait for fluent bit daemonset to be ready after node pool creation
        wait_for_to_be_ready("daemonset", "fluent-bit", self.namespace, 60)

    def setup_bootstrap(self):
        """
        setup bootstrap initializes a session and adds a single bootstrap node
        :return: DeploymentInfo type, containing the settings info of the new node
        """
        bootstrap_deployment_info = DeploymentInfo(self.namespace)

        return setup_bootstrap_in_namespace(self.namespace,
            bootstrap_deployment_info, self.config['bootstrap'],
            self.config['genesis_delta'],
            dep_time_out=self.config['deployment_ready_time_out'])

    def start_poet(self):
        bs_pod = self.bootstrap.pods[0]

        match = pod.search_phrase_in_pod_log(bs_pod['name'], self.namespace, 'poet',
            "REST proxy start listening on 0.0.0.0:80")
        if not match:
            raise Exception("Failed to read container logs in {0}".format("poet"))

        print("Starting PoET")
        out = api_call(bs_pod['pod_ip'],
                '{ "gatewayAddresses": ["127.0.0.1:9092"] }', 'v1/start', self.namespace, "80")
        assert out == "{}", "PoET start returned error {0}".format(out)
        print("PoET started")

    def setup_clients(self):
        """
        setup clients adds new client nodes using suite file specifications

        :return: client_info of type DeploymentInfo
                contains the settings info of the new client node
        """
        client_info = DeploymentInfo(self.namespace)
        client_info = setup_clients_in_namespace(self.namespace,
            self.bootstrap.pods[0],
            client_info,
            self.config['client'],
            self.config['genesis_delta'],
            poet=self.bootstrap.pods[0]['pod_ip'],
            dep_time_out=self.config['deployment_ready_time_out'])

        return client_info

    def __call__(self, result=None):
        """
        Perform the following in order: pre-setup, run test, post-teardown,
        skipping pre/post hooks if test is set to be skipped.

        If debug=True, reraise any errors in setup and use super().debug()
        instead of __call__() to run the test.
        """
        testMethod = getattr(self, self._testMethodName)
        skipped = (
            getattr(self.__class__, "__systemtest_skip__", False) or
            getattr(testMethod, "__systemtest_skip__", False)
        )

        if not skipped and not hasattr(self, 'config'):
            try:
                self.setup()
            except Exception:
                if debug:
                    raise
                result.addError(self, sys.exc_info())
                return
        super().__call__(result)

    def defaultTestResult(self):
        import pdb; pdb.set_trace()
        return AMTestResult(self)

class AMTestResult(unittest.TestResult):
    testcase = None

    def __init__(self, testcase):
        super(AMTestResult, self).__init__()
        self.testcase = testcase

    def stopTestRun(self):
        # dump ES content either if tests has failed of whether is_dump param was set to True in the test config file
        success =  self.wasSuccessful()
        if not success:
            dump_params = {
                "index_date": self.index_date,
                "es_ip": ES(self.namespace).get_elastic_ip(),
                "es_user": conf.ES_USER_LOCAL,
                "es_pass": conf.ES_PASS_LOCAL,
                "main_es_ip": conf.MAIN_ES_IP,
                "dump_queue_name": conf.DUMP_QUEUE_NAME,
                "dump_queue_zone": conf.DUMP_QUEUE_ZONE,
            }
        else:
            dump_params = {}

        queue_params = {
                "project_id": conf.PROJECT_ID,
                "queue_name": conf.TD_QUEUE_NAME,
                "queue_zone": conf.TD_QUEUE_ZONE,
        }
        payload = {
            "namespace": self.namespace,
            "is_delns": True, # str2bool(request.config.getoption("--delns")),
            "is_dump": not success,
            "project_id": conf.PROJECT_ID,
            "pool_name": f"pool-{self.namespace}",
            "cluster_name": conf.CLUSTER_NAME,
            "node_pool_zone": conf.CLUSTER_ZONE,
        }
        create_google_cloud_task(queue_params, payload, **dump_params)
