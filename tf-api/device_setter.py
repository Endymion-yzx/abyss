from tensorflow.python.training.device_setter import replica_device_setter

def abyss_replica_device_setter(ps_tasks=0, ps_device='/job:ps', worker_device='/job:worker'):
	return replica_device_setter(ps_tasks, ps_device, worker_device, merge_devices=True)
