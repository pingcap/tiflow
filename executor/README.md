# Structure

## server

executor-server is in charge of:

- maintain heartbeat with master.
- transfer the tasks from master to runtime engine.

## runtime

The task runtime has some important struct and implements the interface:

- **runtime**, the (singleton) instance organizes and executes tasks.
  - **Run** function that actually drives the execution of the tasks and is thread-safe.
  - *Queue* field that contains the runnable tasks.
- **task**, also called *taskContainer*, wraps the logic of operator and maintains the status and input/output channels.
  - **Poll** triggers a executing action of the task, which will return quickly. During the poll action, the task will
    - read inputs channels, if the channels are empty, return `blocked`. The read action fetches a batch of data.
  - *status* records the current status of task, including
    - `runnable` means the task is in the *Queue* and can run immediately.
    - `blocked` means the task is waiting someone to trigger, say output data consuming, input data arriving or i/o task completed.
    - `awaiting` means this task has been awoken by someone. If the **Poll** function ends and checks this status, we should put this task back to queue and reset the status to runnable.
- **Operator** actually implement the user logic. Every operator is contained by a task. Ideally, operators can construct an operating tree like a typical sql engine.
  - **prepare** is called when constructing the operator and prepare some resources such as workerpool or grpc stream.
  - **next([]\*records)** accepts the incoming data and returns the result data or blocked status.
