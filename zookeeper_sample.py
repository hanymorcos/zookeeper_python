from kazoo.client import KazooClient
import time
import os

# Connect to ZooKeeper
zk = KazooClient(hosts='127.0.0.1:2181')
zk.start()

# Create a parent znode if it doesn't exist
if not zk.exists("/workers"):
    zk.create("/workers", b"", makepath=True)

# Create a worker znode
worker_path = zk.create("/workers/worker-", ephemeral=True, sequence=True)

# Define task handling function
def handle_task(data, stat, event):
    print(f"Worker {os.path.basename(worker_path)} handling task: {data.decode()}")

# Watch the tasks znode for changes
@zk.ChildrenWatch("/tasks")
def watch_tasks(children):
    for task in children:
        task_path = f"/tasks/{task}"
        # Check if this task is already being handled by another worker
        if zk.exists(f"{task_path}/assigned"):
            continue
        # Try to claim the task
        if zk.exists(task_path):
            try:
                zk.create(f"{task_path}/assigned", value=task_path.encode(), ephemeral=True)
                task_data, _ = zk.get(task_path)
                # Perform task
                handle_task(task_data, None, None)
                # Remove task znode when done
                zk.delete(task_path, recursive=True)
            except Exception as e:
                print(f"Error handling task: {e}")

# Create a parent znode if it doesn't exist
if not zk.exists("/tasks"):
    zk.create("/tasks", b"", makepath=True)

# Add tasks to the queue
for i in range(5):
    if not zk.exists("/tasks/task-" + str(i)):
      zk.create("/tasks/task-" + str(i), value=f"Task {i}".encode())

# Keep the program running
try:
    while True:
        time.sleep(1)
except KeyboardInterrupt:
    zk.stop()
