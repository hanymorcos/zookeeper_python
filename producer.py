from kazoo.client import KazooClient
import time
import os

# Connect to ZooKeeper
zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

# Create a parent znode if it doesn't exist
if not zk.exists("/tasks"):
    zk.create("/tasks", b"", makepath=True)

# Add tasks to the queue
for i in range(5):
    if not zk.exists("/tasks/task-" + str(i)):
      zk.create("/tasks/task-" + str(i), value=f"Task {i}".encode())

