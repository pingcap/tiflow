# Structure

## server

executor-server is in charge of:

- maintain heartbeat with master.
- transfer the tasks from master to runtime engine.

## runtime

The task runtime maintains some important structures and implements the interface:

- **runtime**, the *singleton* instance organizes and executes tasks.
  - **Run** function that actually drives the execution of the tasks and is thread-safe.
  - *Queue* field that contains the runnable tasks.
- **taskContainer** wraps the logic of operator and maintains the status and input/output channels.
  - **Poll** triggers a executing action of the task, which will return quickly. During the poll action, the task will
    - read inputs channels, if the channels are empty, return `blocked`. The read action fetches a batch of data.
  - *status* records the current status of task, including
    - `runnable` means the task is in the *Queue* and can run immediately.
    - `blocked` means the task is waiting someone to trigger, say output data consuming, input data arriving or i/o task completed.
    - `awaiting` means this task has been awoken by someone. If the **Poll** function ends and checks this status, we should put this task back to queue and reset the status to runnable.
- **Operator** actually implement the user logic. Every operator is contained by a task. Ideally, operators can construct an operating tree like a typical sql engine.
  - **prepare** is called when constructing the operator and prepare some resources.
  - **close** releases all resources gracefully.
  - **next(ctx \*taskContext, r \*record, index int)** accepts the incoming data and returns the result data or blocked status. `index` indicates which input the record comes from.
  - **NextWantedInputIdx** returns a integer indicating which input the task should read in this poll. In normal cases it returns non-negative value. There are two special value:
    - **DontNeedData** suggests not to read data from input. It happens when the operator reads data from grpc streams or the operator was blocked last time and need to digest current blocking record at this time.
    - **DontRequireData**. Given a union operator, it can read from any inputs without caring about the read order. In this case, we return this status.