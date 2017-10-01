import tensorflow as tf

flags = tf.app.flags
flags.DEFINE_string("ps_hosts","localhost:2222",
                    "One hostname:port pair")
flags.DEFINE_string("worker_hosts", "localhost:2223",
                    "Comma-separated list of hostname:port pairs")
flags.DEFINE_string("coord_host","localhost:2222",
                    "One hostname:port pair")
flags.DEFINE_string("job_name", None,"job name: ps or worker")
flags.DEFINE_integer("task_index", None,
                     "Worker task index, should be >= 0.")
FLAGS = flags.FLAGS

cluster = tf.train.ClusterSpec({'ps': FLAGS.ps_hosts.split(','), 
			'worker': FLAGS.worker_hosts.split(','),
			'coord': [FLAGS.coord_host]})
server = tf.train.Server(cluster, job_name=FLAGS.job_name, task_index=FLAGS.task_index)

server.join()
