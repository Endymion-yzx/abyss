from kubernetes import client, config
import tensorflow as tf
from random import randint

class AbyssSession():
	def __init__(self):
		self._closed = False

		self._coord_port = randint(5000, 10000)
		self._resources = { 'service':[], 'replicaset':[] }
		self._container_name = 'tf-container'
		# TODO: dynamically allocate IP or use DNS
		self._container_IP = '10.100.100.100'
		self._container_port = randint(5000, 10000)
		#self._namespace = 'tensorflow'
		self._namespace = 'default'
		self._image = '495609715/tf_container:v1.0.1'
		self._script = '/container.py'

		# Start a container
		config.load_kube_config()
		service = client.V1Service()
		service.api_version = 'v1'
		service.kind = 'Service'
		service.metadata = client.V1ObjectMeta(name=self._container_name)
		srvSpec = client.V1ServiceSpec()
		srvSpec.selector = { 'name': self._container_name, 'job': 'container', 'task': '0' }
		srvSpec.ports = [client.V1ServicePort(port=self._container_port)]
		srvSpec.cluster_ip = self._container_IP
		service.spec = srvSpec
		api_instance = client.CoreV1Api()
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
		podSpec = client.V1PodSpec()
		container = client.V1Container()
		container.name = 'tensorflow'
		container.image = self._image
		container.ports = [client.V1ContainerPort(self._container_port)]
		container.command = ['/usr/bin/python', self._script]
		container.args = []
		# Command line arguments
		container.args.append('--job_name=container')
		container.args.append('--task_index=0')

		container.args.append('--coord_host=192.168.2.110:'+str(self._coord_port))
		container.args.append('--container_hosts='+self._container_IP+':'+str(self._container_port))

		podSpec.containers = [container]
		template.spec = podSpec
		rsSpec.template = template
		replicaset.spec = rsSpec
		api_instance = client.ExtensionsV1beta1Api()
		api_instance.create_namespaced_replica_set(namespace=self._namespace, body=replicaset)
		self._resources['replicaset'].append(replicaset)

		# Create the cluster
		# TODO: decide the coordinator's address
		cluster = tf.train.ClusterSpec({'coord': ['192.168.2.110:'+str(self._coord_port)], 
			'container': [self._container_IP+':'+str(self._container_port)]})
		print(cluster.__dict__)
		server = tf.train.Server(cluster, job_name='coord', task_index=0)

		# Create a session
		self._sess = tf.Session(server.target)

	def run(self, fetches, feed_dict=None, options=None, run_metadata=None):
		if isinstance(fetches, tf.Tensor):
			g = fetches.graph
			nodes = g._nodes_by_id.values()
			for node in nodes:
				node._set_device('/job:container/task:0')
		else:
			fetch_list = list(fetches)
			for fetch in fetch_list:
				g = fetch.graph
				nodes = g._nodes_by_id.values()
				for node in nodes:
					node._set_device('/job:container/task:0')

		return self._sess.run(fetches, feed_dict, options, run_metadata)

	def close(self):
		self._sess.close()
		# TODO: delete the container
		self._closed = True

	def __del__(self):
		if not self._closed:
			self.close()
