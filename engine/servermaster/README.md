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
      - query job(s)
      - cancel job
    - schedule and failover jobs
- ExecutorManager
  - Handle Heartbeat
    - Notify Resource Manager updates the status infos
  - Maintain the aliveness of Executors.
    - Check liveness
      - Executor Manager check every executor whether the heartbeat has been timeout.
      - Once an executor is offline by `heartbeat timeout`, it should notify the job manager to reschedule all the tasks on it.
- Executor Client
  - Embeds two independent interfaces
      - `ExecutorServiceClient`, which is used to dispatch task to executor.
      - `BrokerServiceClient`, which is used to manage resource belongs to an executor.
- JobManager
  - Receive SubmitJob Request, Check the type of Job, Create the JobMaster.
  - JobMaster (per job)
    - Generate the Tasks for the job
    - Schedule and Dispatch Tasks
