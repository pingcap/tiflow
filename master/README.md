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
- ExecutorManager
  - Handle Heartbeat
    - Notify Resource Manager updates the status infos
  - Maintain the stats of Executors
  - Once an executor is offline, it should notify the job manager to reschedule all the tasks on it.
- Executor Client
  - communicate with an executor. If meet transilient fault, it will set executor as `Disconnected`
- ResourceManager
  - ResourceManager is an interface but implemented by Executor Manager
  - The Cost Model is supposed to have two types
    - Expected Usage
    - Real Usage
  - The Occupied resources in `Resource Manager` should be `sum(max(expected usage, real usage)`. The real usage will be updated by heartbeats.
- JobManager
  - Receive SubmitJob Request, Check the type of Job, Create the JobMaster and Build Plan.
  - JobMaster (per job)
    - Generate the Tasks for the job
    - Schedule and Dispatch Tasks
      1. Receive the Dispatch Job Request
      2. Get `ResourceSnapshot` from Resource Manager
      3. Assign tasks to different executors, get `SubJob - Tasks - Executor` tuple.
          1. if resource is not enough, get back to step 2.
      4. Dispatch the tasks.
         - If failed, go back to step 2.
         - If success, Update HA Store.
      5. Start working thread.
    - Periodically check whether to reschedule the tasks.
