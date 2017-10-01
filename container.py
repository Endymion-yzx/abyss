import tensorflow as tf

flags = tf.app.flags

flags.DEFINE_string("job_name", None, "job name in the cluster")
flags.DEFINE_integer("task_index", None,
                     "Worker task index, should be >= 0.")
flags.DEFINE_string("cluster_spec", None, "A dictionary mapping jobs with hosts")
FLAGS = flags.FLAGS

cluster = tf.train.ClusterSpec(eval(FLAGS.cluster_spec))
server = tf.train.Server(cluster, job_name=FLAGS.job_name, task_index=FLAGS.task_index)

server.join()
