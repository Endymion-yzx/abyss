# Training a linear regression model
from abyss_session import *
import tensorflow as tf

with tf.device('/job:ps/task:0'):
	W = tf.Variable(tf.truncated_normal([2, 1]))
	b = tf.Variable(tf.truncated_normal([1]))
	init_op = tf.global_variables_initializer()

with tf.device('/job:worker/task:0'):
	x = tf.placeholder(tf.float32, [None, 2])   # Each row represent a sample
	true_y = tf.placeholder(tf.float32, [None, 1])
	y = tf.matmul(x, W) + b

with tf.device('/job:worker/task:1'):
	cost = tf.reduce_mean(tf.square(true_y - y))
	train_op = tf.train.GradientDescentOptimizer(0.01).minimize(cost)

sess = AbyssSession(['worker', 'ps'], [2, 1])

x_data = [[1.0, 2.0], [3.0, 4.1], [5.0, 5.8], [7.0, 8.0], [9.3, 10.0]]
y_data = [[0.1], [0.2], [0.3], [0.4], [0.5]]

sess.run(init_op)
print sess.run(cost, {x: x_data, true_y: y_data})
for i in range(100):
	sess.run(train_op, {x: x_data, true_y: y_data})
print sess.run(cost, {x: x_data, true_y: y_data})

sess.close()
