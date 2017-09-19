from kubernetes import client, config
import tensorflow as tf
from random import randint

class AbyssSession():
	def __init__(self):
		self._closed = False

		self._resources = { 'service':[], 'replicaset':[] }
		self._worker_name = 'tf-worker'
		self._worker_port = randint(5000, 10000)
		self._namespace = 'tensorflow'
		self._image = '495609715/tf_worker:v1'
		self._script = 'worker.py'

		# Start a container
		service = client.V1Service()
		service.api_version = 'v1'
		service.kind = 'Service'
		service.metadata = client.V1ObjectMeta(name=self._worker_name)
		srvSpec = client.V1ServiceSpec()
		srvSpec.selector = { 'name': self._name, 'job': 'worker', 'task': '0' }
		srvSpec.ports = [client.V1ServicePort(port=self._worker_port)]
		service.spec = srvSpec
		api_instance = client.CoreV1Api()
		api_instance.create_namespaced_service(namespace=self._namespace, body=service)
		self._resources['service'].append(service)

		replicaset = client.V1beta1ReplicaSet()
		replicaset.api_version = 'extensions/v1beta1'
		replicaset.kind = 'ReplicaSet'
		replicaset.metadata = client.V1ObjectMeta(name=self._worker_name)
		rsSpec = client.V1beta1ReplicaSetSpec()
		rsSpec.replicas = 1

		template = client.V1PodTemplateSpec()
		template.metadata = client.V1ObjectMeta(labels={'name':self._worker_name, 'job':'worker', 'task':'0'})
		podSpec = client.V1PodSpec()
		container = client.V1Container()
		container.name = 'tensorflow'
		container.image = self._image
		container.ports = [client.V1ContainerPort(self._worker_port)]
		container.command = ['/usr/bin/python', self._script]
		container.args = []
		# TODO: include the address of the coordinator in the arguments
		# container.args.append('--job_name='+job)
		# container.args.append('--task_index='+str(i))

		# container.args.append(worker_arg)
		# container.args.append(ps_arg)

		podSpec.containers = [container]
		template.spec = podSpec
		rsSpec.template = template
		replicaset.spec = rsSpec
		api_instance = client.ExtensionsV1beta1Api()
		api_instance.create_namespaced_replica_set(namespace=self._namespace, body=replicaset)
		self._resources['replicaset'].append(replicaset)

		# Create the cluster
		cluster = tf.train.ClusterSpec({'coord': ['XXX'], 
			'worker': [self._worker_name+':'+str(self._worker_port)]})
		server = tf.train.Server(cluster, job_name='coord', task_index=0)

		# Create a session
		self._sess = tf.Session(server.target)

	def run(self, fetches, feed_dict=None, options=None, run_metadata=None):
		if isinstance(fetches, tf.Tensor):
			g = fetches.graph
		else:
			fetch_list = list(fetches)
			g = fetch_list[0].graph

		nodes = g._nodes_by_id.values()
		for node in nodes:
			node._set_device('/job:worker/task:0')

		return self._sess.run(fetches, feed_dict, options, run_metadata)

	def close():
		self._sess.close()
		# TODO: delete the container
		self._closed = True

	def __del__():
		if not self._closed:
			self.close()
