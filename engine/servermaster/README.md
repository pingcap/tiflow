# Structure of Master

## Structure

The master module consists of several components:

- Master Server.
  - provide Grpc/http api, including
    - operate cluster
      - register/delete executor server
      - handle heartbeat (keep alive)
    - user interface
      - submit job
      - update job
      - delete job
    - schedule jobs
- ExecutorManager
  - Handle Heartbeat
    - Notify Resource Manager updates the status infos
  - Maintain the stats of Executors. There are several Status for every Executor.
    - **Initing** means the executor is under initialization. It happens when executor has sent `Register Executor` request but has yet sent Heartbeat.
    - **Running** means executor is running and is able to receive more tasks.
    - **Busy** means executor is running but cannot receive more tasks.
    - **Disconnected** happens when master sends a request but meets I/O fails.
    - **Tombstone** this executor has been disconnected for a while. We regard it has been dead.
  - **Check liveness**.
    - Executor Manager check every executor whether the heartbeat has been timeout.
    - Once an executor is offline by `heartbeat timeout`, it should notify the job manager to reschedule all the tasks on it.
- Executor Client
  - Communicate with an executor, such as `SubmitTasks`. If meet transilient fault, it will set executor as `Disconnected`
- ResourceManager
  - ResourceManager is an interface but implemented by Executor Manager
  - The Cost Model is supposed to have two types
    - Expected Usage
    - Real Usage
  - The Occupied resources in `Resource Manager` should be `sum(max(expected usage, real usage)`. The real usage will be updated by heartbeats.
- Scheduler
  - It serves as a service that designates tasks to different executors. It implements multiple allocation strategies.
- JobManager
  - Receive SubmitJob Request, Check the type of Job, Create the JobMaster and Build Plan.
  - JobMaster (per job)
    - Generate the Tasks for the job
    - Schedule and Dispatch Tasks
      1. Receive the Dispatch Job Request
      2. Send schedule request to scheduler, get `SubJob - Tasks - Executor` tuple.
         - if resource is not enough, get back to step 2.
      3. Start working thread.
      4. Dispatch the tasks.
         - If failed, go back to step 2.
         - If success, Update HA Store.
