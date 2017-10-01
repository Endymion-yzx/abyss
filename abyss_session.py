from kubernetes import client, config
import tensorflow as tf
from random import randint
import getpass
import subprocess
import re

class AbyssSession():
	def __init__(self):
		self._closed = False

		self._ps_replicas = 1
		self._worker_replicas = 2
		self._replicas = {"ps": self._ps_replicas, "worker": self._worker_replicas}
		self._worker_port = randint(5000, 10000)
		self._coord_port = randint(5000, 10000)
		self._resources = { 'service':[], 'replicaset':[] }
		self._container_name = getpass.getuser() + '-tf-container'
		self._worker_hosts = []
		self._ps_hosts = []
		for i in range(self._worker_replicas):
			#worker_name = self._container_name + '-worker-' + str(i) + ':' + str(self._port)
			worker_addr = '10.100.100.' + str(i) + ':' + str(self._worker_port)
			self._worker_hosts.append(worker_addr)

		for j in range(self._ps_replicas):
			#ps_name = self._container_name + '-ps-' + str(j) + ':' + str(self._port)
			ps_addr = '10.100.101.' + str(j) + ':' + str(self._worker_port)
			self._ps_hosts.append(ps_addr)
		# TODO: dynamically allocate IP or use DNS
		#self._namespace = 'tensorflow'
		self._namespace = 'default'
		self._image = '495609715/tf_container:v1.0.2'
		self._script = '/container.py'

		self._job_map = {}      # Match machines required by user with containers
		for job in ['worker', 'ps']:
			ctn_job = 'container_' + job
			self._job_map[job] = ctn_job
		self._cluster_spec = {
			self._job_map['worker']: self._worker_hosts, 
			self._job_map['ps']: self._ps_hosts, 
			# TODO: decide the coordinator's address
			'coord': ['192.168.2.110:'+str(self._coord_port)]}
		
		for job in ['worker', 'ps']:

			for i in range(self._replicas[job]):
				# Start a container
				config.load_kube_config()
				service = client.V1Service()
				service.api_version = 'v1'
				service.kind = 'Service'
				service.metadata = client.V1ObjectMeta(name=(self._container_name + '-' + job + '-' + str(i)))
				srvSpec = client.V1ServiceSpec()
				srvSpec.selector = { 'name': self._container_name + '-' + job + '-' + str(i), 'job': job, 'task': str(i) }
				srvSpec.ports = [client.V1ServicePort(port=self._worker_port)]
				if job == 'worker':
					srvSpec.cluster_ip = self._worker_hosts[i].split(':')[0]
				if job == 'ps':
					srvSpec.cluster_ip = self._ps_hosts[i].split(':')[0]
				service.spec = srvSpec

				api_instance = client.CoreV1Api()
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
				podSpec = client.V1PodSpec()
				container = client.V1Container()
				container.name = 'tensorflow'
				container.image = self._image
				container.ports = [client.V1ContainerPort(self._worker_port)]
				container.command = ['/usr/bin/python', self._script]
				container.args = []
				# Command line arguments
				container.args.append('--job_name=' + self._job_map[job])
				container.args.append('--task_index=' + str(i))

				container.args.append('--cluster_spec=' + str(self._cluster_spec))
				print 'args:'
				print container.args
				podSpec.containers = [container]
				template.spec = podSpec
				rsSpec.template = template
				replicaset.spec = rsSpec
				api_instance_rs = client.ExtensionsV1beta1Api()
				api_instance_rs.create_namespaced_replica_set(namespace=self._namespace, body=replicaset)
				self._resources['replicaset'].append(replicaset)

		# Create the cluster
		cluster = tf.train.ClusterSpec(self._cluster_spec)
		print 'cluster:'
		print(cluster.__dict__)
		server = tf.train.Server(cluster, job_name='coord', task_index=0)

		# Create a session
		self._sess = tf.Session(server.target)

	def run(self, fetches, feed_dict=None, options=None, run_metadata=None):
		graph = self._sess.graph
		nodes = graph._nodes_by_id.values()
		for node in nodes:
			device = node.device        # What if device is empty?
			if re.search(r'job:([^\/]*)', device):
				node._set_device(re.sub(r'job:([^\/]*)', 
				lambda match: 'job:' + self._job_map[match.group(1)], device))

		return self._sess.run(fetches, feed_dict, options, run_metadata)

	def close(self):
		self._sess.close()

		api_instance = client.CoreV1Api()
		for service in self._resources['service']:
			api_instance.delete_namespaced_service(name=service.metadata.name, namespace='default')

		api_instance = client.ExtensionsV1beta1Api()
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
