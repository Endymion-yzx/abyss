from tf_api import *

mission = TensorFlowMission()
mission.name = 'api-test'
mission.script = '/mnist.py'
mission.num_ps = 2
mission.num_worker = 2
mission.data_dir = '/home/zyang/data'
mission.train_dir = '/home/zyang/train'

mission.launch()

print mission.log('worker', 0)
print mission.log('worker', 1)
