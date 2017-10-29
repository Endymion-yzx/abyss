from kubernetes import client, config
from tensorflow.python.training.server_lib import Server
from tensorflow.python.training.server_lib import ClusterSpec
from tensorflow.python.client.session import Session
from random import randint
import getpass
import subprocess
import re

class AbyssSingleSession():
	def __init__(self):
		self._closed = False

		# Configure API key authorization: BearerToken
		self._configuration = client.Configuration()
		with open('/var/run/secrets/kubernetes.io/serviceaccount/token', 'r') as f:
			token = f.read()
			self._configuration.api_key['authorization'] = token
		# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
		self._configuration.api_key_prefix['authorization'] = 'Bearer'
		self._configuration.ssl_ca_cert = '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt'
		url = 'https://kubernetes.default.svc'
		self._configuration.host = url

		self._resources = { 'service':[], 'replicaset':[] }
		self._coord_name = 'my-notebook'
		# self._coord_port = randint(5000, 30000)
		self._coord_port = 7777       # Currently fixed in the notebook yaml file
		self._container_name = 'tf-container'
		self._container_port = randint(5000, 30000)
		# TODO: Use namespace to isolate users
		self._namespace = 'default'
		self._image = '495609715/tf_container:v1.0.2'
		self._script = '/container.py'

		# Start a container
		service = client.V1Service()
		service.api_version = 'v1'
		service.kind = 'Service'
		service.metadata = client.V1ObjectMeta(name=self._container_name)
		srvSpec = client.V1ServiceSpec()
		srvSpec.selector = { 'name': self._container_name, 'job': 'container', 'task': '0' }
		srvSpec.ports = [client.V1ServicePort(port=self._container_port)]
		service.spec = srvSpec
		api_instance = client.CoreV1Api(client.ApiClient(configuration=self._configuration))
		api_instance.create_namespaced_service(namespace=self._namespace, body=service)
		self._resources['service'].append(service)

		replicaset = client.V1beta1ReplicaSet()
		replicaset.api_version = 'extensions/v1beta1'
		replicaset.kind = 'ReplicaSet'
		replicaset.metadata = client.V1ObjectMeta(name=self._container_name)
		rsSpec = client.V1beta1ReplicaSetSpec()
		rsSpec.replicas = 1

		template = client.V1PodTemplateSpec()
		template.metadata = client.V1ObjectMeta(labels={'name':self._container_name, 'job':'container', 'task':'0'})
		container = client.V1Container(name='tensorflow')
		container.image = self._image
		container.ports = [client.V1ContainerPort(self._container_port)]
		container.command = ['/usr/bin/python', self._script]
		container.args = []
		# Command line arguments
		container.args.append('--job_name=container')
		container.args.append('--task_index=0')

		self._cluster_spec = {'container': [self._container_name+':'+str(self._container_port)], 
			'coord': [self._coord_name+':'+str(self._coord_port)]}
		container.args.append('--cluster_spec=' + str(self._cluster_spec))
		#print('args:')
		#print(container.args)
		podSpec = client.V1PodSpec(containers=[container])
		template.spec = podSpec
		rsSpec.template = template
		replicaset.spec = rsSpec
		api_instance = client.ExtensionsV1beta1Api(client.ApiClient(configuration=self._configuration))
		api_instance.create_namespaced_replica_set(namespace=self._namespace, body=replicaset)
		self._resources['replicaset'].append(replicaset)

		# Create the cluster
		cluster = ClusterSpec(self._cluster_spec)
		#print(cluster.__dict__)
		server = Server(cluster, job_name='coord', task_index=0)

		# Create a session
		self._sess = Session(server.target)

		print(self._resources)

	def run(self, fetches, feed_dict=None, options=None, run_metadata=None):
		graph = self._sess.graph
		nodes = graph._nodes_by_id.values()
		for node in nodes:
			node._set_device('/job:container/task:0')

		return self._sess.run(fetches, feed_dict, options, run_metadata)

	def close(self):
		self._sess.close()

		api_instance = client.CoreV1Api(client.ApiClient(configuration=self._configuration))
		for service in self._resources['service']:
			api_instance.delete_namespaced_service(name=service.metadata.name, namespace='default')

		api_instance = client.ExtensionsV1beta1Api(client.ApiClient(configuration=self._configuration))
		for replicaset in self._resources['replicaset']:
			clearScale = client.ExtensionsV1beta1Scale(
				api_version='extensions/v1beta1',
				kind='Scale',
				metadata=client.V1ObjectMeta(name=replicaset.metadata.name, namespace='default'),
				spec=client.ExtensionsV1beta1ScaleSpec(replicas=0)) 

			api_instance.replace_namespaced_replica_set_scale(
				name=replicaset.metadata.name,
				namespace='default',
				body=clearScale)
			
			api_instance.delete_namespaced_replica_set(name=replicaset.metadata.name, namespace='default', body=client.V1DeleteOptions())

		self._resources = { 'service': [], 'replicaset': [] }
		self._closed = True

	def __del__(self):
		if not self._closed:
			self.close()

class AbyssDistributedSession():
	def __init__(self, jobs, replicas):
		"""
		:param jobs: List of names of jobs comprising the tensorflow cluster
		:param replicas: List of numbers of tasks in each job
		"""

		self._closed = False

		# Configure API key authorization: BearerToken
		self._configuration = client.Configuration()
		with open('/var/run/secrets/kubernetes.io/serviceaccount/token', 'r') as f:
			token = f.read()
			self._configuration.api_key['authorization'] = token
		# Uncomment below to setup prefix (e.g. Bearer) for API key, if needed
		self._configuration.api_key_prefix['authorization'] = 'Bearer'
		self._configuration.ssl_ca_cert = '/var/run/secrets/kubernetes.io/serviceaccount/ca.crt'
		url = 'https://kubernetes.default.svc'
		self._configuration.host = url

		self._jobs = jobs
		self._replicas = dict(zip(jobs, replicas))
		self._resources = { 'service':[], 'replicaset':[] }
		self._container_port = randint(5000, 30000)
		# self._coord_port = randint(5000, 30000)
		self._coord_port = 7777      # Currently fixed
		self._coord_name = 'my-notebook'
		self._container_name = 'tf-container'

		self._hosts = {}
		for i, job in enumerate(self._jobs):
			self._hosts[job] = []
			for j in range(self._replicas[job]):
				addr = self._container_name + '-' + job + '-' + str(j) + ':' + str(self._container_port)
				self._hosts[job].append(addr)
		
		# TODO: Use namespace to isolate users
		self._namespace = 'default'
		self._image = '495609715/tf_container:v1.0.2'
		self._script = '/container.py'

		self._job_map = {}	  # Match machines required by user with containers
		for job in self._jobs:
			ctn_job = 'ctn_' + job
			self._job_map[job] = ctn_job
		self._cluster_spec = {}
		for job in self._jobs:
			self._cluster_spec[self._job_map[job]] = self._hosts[job]
		self._cluster_spec['coord'] = [self._coord_name + ':' + str(self._coord_port)]
		
		for job in self._jobs:

			for i in range(self._replicas[job]):
				service = client.V1Service()
				service.api_version = 'v1'
				service.kind = 'Service'
				service.metadata = client.V1ObjectMeta(name=(self._container_name + '-' + job + '-' + str(i)))
				srvSpec = client.V1ServiceSpec()
				srvSpec.selector = { 'name': self._container_name + '-' + job + '-' + str(i), 'job': job, 'task': str(i) }
				srvSpec.ports = [client.V1ServicePort(port=self._container_port)]
				service.spec = srvSpec

				api_instance = client.CoreV1Api(client.ApiClient(configuration=self._configuration))
				api_instance.create_namespaced_service(namespace=self._namespace, body=service)
				self._resources['service'].append(service)

				replicaset = client.V1beta1ReplicaSet()
				replicaset.api_version = 'extensions/v1beta1'
				replicaset.kind = 'ReplicaSet'
				replicaset.metadata = client.V1ObjectMeta(name=(self._container_name + '-' + job + '-' + str(i)))
				rsSpec = client.V1beta1ReplicaSetSpec()
				rsSpec.replicas = 1

				template = client.V1PodTemplateSpec()
				template.metadata = client.V1ObjectMeta(labels={'name':self._container_name + '-' + job + '-' + str(i), 'job':job, 'task':str(i)})
				container = client.V1Container(name='tensorflow')
				container.image = self._image
				container.ports = [client.V1ContainerPort(self._container_port)]
				container.command = ['/usr/bin/python', self._script]
				container.args = []
				# Command line arguments
				container.args.append('--job_name=' + self._job_map[job])
				container.args.append('--task_index=' + str(i))

				container.args.append('--cluster_spec=' + str(self._cluster_spec))
				#print('args:')
				#print(container.args)
				podSpec = client.V1PodSpec(containers=[container])
				template.spec = podSpec
				rsSpec.template = template
				replicaset.spec = rsSpec
				api_instance_rs = client.ExtensionsV1beta1Api(client.ApiClient(configuration=self._configuration))
				api_instance_rs.create_namespaced_replica_set(namespace=self._namespace, body=replicaset)
				self._resources['replicaset'].append(replicaset)

		# Create the cluster
		cluster = ClusterSpec(self._cluster_spec)
		#print('cluster:')
		#print(cluster.__dict__)
		server = Server(cluster, job_name='coord', task_index=0)

		# Create a session
		self._sess = Session(server.target)

	def run(self, fetches, feed_dict=None, options=None, run_metadata=None):
		graph = self._sess.graph
		nodes = graph._nodes_by_id.values()
		for node in nodes:
			device = node.device		# What if device is empty?
			res = re.search(r'job:([^\/]*)', device)
			if res and res.group(1) in self._job_map:
				node._set_device(re.sub(res.group(1), self._job_map[res.group(1)], device))

		return self._sess.run(fetches, feed_dict, options, run_metadata)

	def close(self):
		self._sess.close()

		api_instance = client.CoreV1Api(client.ApiClient(configuration=self._configuration))
		for service in self._resources['service']:
			api_instance.delete_namespaced_service(name=service.metadata.name, namespace='default')

		api_instance = client.ExtensionsV1beta1Api(client.ApiClient(configuration=self._configuration))
		for replicaset in self._resources['replicaset']:
			clearScale = client.ExtensionsV1beta1Scale(
				api_version='extensions/v1beta1',
				kind='Scale',
				metadata=client.V1ObjectMeta(name=replicaset.metadata.name, namespace='default'),
				spec=client.ExtensionsV1beta1ScaleSpec(replicas=0)) 

			api_instance.replace_namespaced_replica_set_scale(
				name=replicaset.metadata.name,
				namespace='default',
				body=clearScale)
			
			api_instance.delete_namespaced_replica_set(name=replicaset.metadata.name, namespace='default', body=client.V1DeleteOptions())

		self._resources = { 'service': [], 'replicaset': [] }
		self._closed = True

	def __del__(self):
		if not self._closed:
			self.close()
