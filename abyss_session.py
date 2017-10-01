from kubernetes import client, config
import tensorflow as tf
from random import randint
import getpass
import subprocess

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
                    worker_name = '10.100.100.11' + str(i) + ':' + str(self._worker_port)
                    self._worker_hosts.append(worker_name)

                for j in range(self._ps_replicas):
                    #ps_name = self._container_name + '-ps-' + str(j) + ':' + str(self._port)
                    ps_name = '10.100.100.11' + str(j+self._worker_replicas) + ':' + str(self._worker_port)
                    self._ps_hosts.append(ps_name)
		# TODO: dynamically allocate IP or use DNS
		#self._namespace = 'tensorflow'
		self._namespace = 'default'
		self._image = 'ywanggf/multi_container:v2'
		self._script = '/container.py'
                
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
			container.args.append('--job_name=' + job)
			container.args.append('--task_index=' + str(i))

			container.args.append('--ps_hosts='+','.join(map(str,self._ps_hosts)))
			container.args.append('--worker_hosts='+','.join(map(str,self._worker_hosts)))
                        container.args.append('--coord_host=192.168.2.110:'+str(self._coord_port))
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
		# TODO: decide the coordinator's address
		cluster = tf.train.ClusterSpec({'worker': self._worker_hosts, 'ps': self._ps_hosts, 'coord': ['192.168.2.110:'+str(self._coord_port)]})
		print 'cluster:'
		print(cluster.__dict__)
		server = tf.train.Server(cluster, job_name='coord', task_index=0)

		# Create a session
		self._sess = tf.Session(server.target)

	def run(self, fetches, feed_dict=None, options=None, run_metadata=None):
		if isinstance(fetches, tf.Tensor):
			g = fetches.graph
			nodes = g._nodes_by_id.values()
			for node in nodes:
				node._set_device('/job:worker/task:0')
		else:
			fetch_list = list(fetches)
			for fetch in fetch_list:
				g = fetch.graph
				nodes = g._nodes_by_id.values()
				for node in nodes:
					node._set_device('/job:worker/task:0')

		return self._sess.run(fetches, feed_dict, options, run_metadata)

	def close(self):
		self._sess.close()
		# Delete service and replicaset
                for job in ['worker', 'ps']:
                    for i in range(self._replicas[job]):
			cmd = 'kubectl delete service,rs ' + self._container_name + '-' + job + '-' + str(i) 
			subprocess.call(cmd, shell=True)
		self._closed = True

	def __del__(self):
		if not self._closed:
			self.close()
