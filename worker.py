import tensorflow as tf

cluster = tf.train.ClusterSpec({'coord': ['XXX'], 
			'worker': [self._worker_name+':'+str(self._worker_port)]})
server = tf.train.Server(cluster, job_name='worker', task_index=0)

server.join()