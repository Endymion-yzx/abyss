from kubernetes import client, config
from random import randint

class TensorFlowMission(object):
	def __init__(self, name=None, script=None, image=None, arch=None, num_ps=None, num_worker=None, port=None, data_dir=None, train_dir=None):
		self._name = name
		self._script = script
		self._image = image
		self._arch = arch
		self._num_ps = num_ps
		self._num_worker = num_worker
		self._port = port
		self._data_dir = data_dir
		self._train_dir = train_dir

		if self._arch == None:
			self._arch = 'parameter-server'         # Default

	@property
	def name(self):
		return self._name

	@name.setter
	def name(self, name):
		self._name = name

	@property
	def script(self):
		return self._script

	@script.setter
	def script(self, script):
		self._script = script

	@property
	def image(self):
		return self._image

	@image.setter
	def image(self, image):
		self._image = image

	@property
	def arch(self):
		return self._arch

	@arch.setter
	def arch(self, arch):
		self._arch = arch

	@property
	def num_ps(self):
		return self._num_ps

	@num_ps.setter
	def num_ps(self, num_ps):
		self._num_ps = num_ps

	@property
	def num_worker(self):
		return self._num_worker

	@num_worker.setter
	def num_worker(self, num_worker):
		self._num_worker = num_worker

	@property
	def port(self):
		return self._port

	@port.setter
	def port(self, port):
		self._port = port

	@property
	def data_dir(self):
		return self._data_dir

	@data_dir.setter
	def data_dir(self, data_dir):
		self._data_dir = data_dir

	@property
	def train_dir(self):
		return self._train_dir

	@train_dir.setter
	def train_dir(self, train_dir):
		self._train_dir = train_dir

	def launch(self):
		if not self._name:
			print 'Name of job not specified'
			return
		if not self._script:
			print 'Script not specified'
			return
		if not self._image:
			print 'Image not specified'
			return

		if self._arch == 'parameter-server':
			if not self._num_ps:
				print 'Number of parameter servers not specified'
				return
			if not self._num_worker:
				print 'Number of workers not specified'
				return
			if not self._port:
				# Get an available port
				self._port = randint(5000, 10000)
			if not self._data_dir:
				# Should correspond to parameters in tf script. Need further consideration
				pass
			if not self._train_dir:
				# Should correspond to parameters in tf script. Need further consideration
				pass

			config.load_kube_config()
			self._resources = { 'service': [], 'replicaset': [] }

			job_name = [ 'ps', 'worker' ]
			job_num = { 'ps': self._num_ps, 'worker': self._num_worker }

			worker_arg = '--worker_hosts='
			for i in range(self._num_worker):
				worker_arg = worker_arg + self._name + '-worker-' + str(i) + ':' + str(self._port)
				if i < self._num_worker - 1:
					worker_arg = worker_arg + ','
			ps_arg = '--ps_hosts='
			for i in range(self._num_ps):
				ps_arg = ps_arg + self._name + '-ps-' + str(i) + ':' + str(self._port)
				if i < self._num_ps - 1:
					ps_arg = ps_arg + ','

			for job in job_name:
				for i in range(job_num[job]):
					service = client.V1Service()
					service.api_version = 'v1'
					service.kind = 'Service'
					service.metadata = client.V1ObjectMeta(name=(self._name+'-'+job+'-'+str(i)))
					srvSpec = client.V1ServiceSpec()
					srvSpec.selector = { 'name': self._name, 'job': job, 'task': str(i) }
					srvSpec.ports = [client.V1ServicePort(port=self._port)]
					service.spec = srvSpec
					api_instance = client.CoreV1Api()
					api_instance.create_namespaced_service(namespace='default', body=service)
					self._resources['service'].append(service)

					replicaset = client.V1beta1ReplicaSet()
					replicaset.api_version = 'extensions/v1beta1'
					replicaset.kind = 'ReplicaSet'
					replicaset.metadata = client.V1ObjectMeta(name=(self._name+'-'+job+'-'+str(i)))
					rsSpec = client.V1beta1ReplicaSetSpec()
					rsSpec.replicas = 1

					template = client.V1PodTemplateSpec()
					template.metadata = client.V1ObjectMeta(labels={'name':self._name, 'job':job, 'task':str(i)})
					podSpec = client.V1PodSpec()
					container = client.V1Container()
					container.name = 'tensorflow'
					container.image = self._image
					container.ports = [client.V1ContainerPort(self._port)]
					container.command = ['/usr/bin/python', self._script]
					container.args = []
					container.args.append('--job_name='+job)
					container.args.append('--task_index='+str(i))
					if self._data_dir:
						container.args.append('--data_dir='+self._data_dir)
					if self._train_dir:
						container.args.append('--train_dir='+self._train_dir)
					container.args.append(worker_arg)
					container.args.append(ps_arg)

					container.volume_mounts = []
					podSpec.volumes = []
					if self._data_dir:
						container.volume_mounts.append(
							client.V1VolumeMount(
								name='data',
								mount_path=self._data_dir,
								read_only=True))
						podSpec.volumes.append(
							client.V1Volume(
								name='data',
								host_path=client.V1HostPathVolumeSource(self._data_dir)))
					if self._train_dir:
						container.volume_mounts.append(
							client.V1VolumeMount(
								name='train',
								mount_path=self._train_dir,
								read_only=False))
						podSpec.volumes.append(
							client.V1Volume(
								name='train',
								host_path=client.V1HostPathVolumeSource(self._train_dir)))

					podSpec.containers = [container]
					template.spec = podSpec
					rsSpec.template = template
					replicaset.spec = rsSpec
					api_instance = client.ExtensionsV1beta1Api()
					api_instance.create_namespaced_replica_set(namespace='default', body=replicaset)
					self._resources['replicaset'].append(replicaset)

	def reset(self):
		if self._resources:
			api_instance = client.CoreV1Api()
			for service in self._resources['service']:
				api_instance.delete_namespaced_service(name=service.metadata.name, namespace='default')
			api_instance = client.ExtensionsV1beta1Api()
			for replicaset in self._resources['replicaset']:
				api_instance.delete_namespaced_replica_set(name=replicaset.metadata.name, namespace='default', body=client.V1DeleteOptions())

	def __del__(self):
		self.reset()

	def __get_pods(self):
		# if self._resources:
		# 	api_instance = client.CoreV1Api()
		# 	for service in self._resources['service']:
		pass


	def status(self):
		pass

	def log(self):
		pass

class TensorFlowClient():
	pass
